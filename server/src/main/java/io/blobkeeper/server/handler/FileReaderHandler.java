package io.blobkeeper.server.handler;

import com.ning.http.util.DateUtil;
import io.blobkeeper.common.domain.ErrorCode;
import io.blobkeeper.common.domain.api.ReturnValue;
import io.blobkeeper.file.domain.File;
import io.blobkeeper.file.service.FileStorage;
import io.blobkeeper.file.util.FileUtils;
import io.blobkeeper.index.domain.IndexElt;
import io.blobkeeper.index.service.IndexService;
import io.blobkeeper.server.handler.api.RequestMapper;
import io.blobkeeper.server.util.MetadataParser;
import io.blobkeeper.server.util.UnClosableFileRegion;
import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.util.Date;

import static com.ning.http.util.DateUtil.formatDate;
import static com.ning.http.util.DateUtil.parseDate;
import static io.blobkeeper.common.domain.Error.createError;
import static io.blobkeeper.common.domain.ErrorCode.*;
import static io.blobkeeper.server.util.HttpUtils.NOT_FOUND;
import static io.blobkeeper.server.util.HttpUtils.*;
import static io.netty.channel.ChannelFutureListener.CLOSE;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Values.KEEP_ALIVE;
import static io.netty.handler.codec.http.HttpHeaders.isKeepAlive;
import static io.netty.handler.codec.http.HttpHeaders.setContentLength;
import static io.netty.handler.codec.http.HttpMethod.*;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static io.netty.handler.codec.http.LastHttpContent.EMPTY_LAST_CONTENT;
import static org.joda.time.DateTimeZone.UTC;

/*
 * Copyright (C) 2015 by Denis M. Gabaydulin
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

@Singleton
@ChannelHandler.Sharable
public class FileReaderHandler extends BaseFileHandler<FullHttpRequest> {
    private static final Logger log = LoggerFactory.getLogger(FileReaderHandler.class);

    private static final int EXPIRE_YEARS = 1;

    @Inject
    private Provider<FileWriterHandler> fileWriterHandlerProvider;

    @Inject
    private IndexService indexService;

    @Inject
    private FileStorage fileStorage;

    @Inject
    private RequestMapper requestMapper;

    @Override
    protected void channelRead0(ChannelHandlerContext context, FullHttpRequest request) throws Exception {
        setContext();

        if (request.getMethod() == POST) {
            context.fireChannelRead(request.copy());
            return;
        }

        if (request.getMethod() == DELETE) {
            context.fireChannelRead(request.copy());
            return;
        }

        addWriterBack(context);

        if (log.isTraceEnabled()) {
            log.trace("Request is: {}", request);
        }

        if (request.getUri().equals("/favicon.ico")) {
            sendError(context, HttpResponseStatus.NOT_FOUND, createError(INVALID_REQUEST, "No favorite icon here"));
            return;
        }

        if (!request.getDecoderResult().isSuccess()) {
            sendError(context, BAD_REQUEST, createError(INVALID_REQUEST, "Strange request given"));
            return;
        }

        if (request.getMethod() != GET) {
            sendError(context, METHOD_NOT_ALLOWED, createError(INVALID_REQUEST, "Only GET requests are acceptable"));
            return;
        }

        String uri = request.getUri();
        final long fileId = getId(uri);
        final int typeId = getType(uri);

        if (NOT_FOUND == fileId) {
            if (tryHandleApiRequest(context, request)) {
                return;
            } else {
                log.error("No id");
                sendError(context, BAD_REQUEST, createError(INVALID_REQUEST, "No id"));
                return;
            }
        }

        if (NOT_FOUND == typeId) {
            log.error("No type id");
            sendError(context, BAD_REQUEST, createError(INVALID_REQUEST, "No type id"));
            return;
        }

        File readerFile = null;
        IndexElt indexElt;
        boolean modified;
        try {
            log.debug("Id {}", fileId);

            indexElt = indexService.getById(fileId, typeId);
            log.debug("Index elt is {}", indexElt);

            if (null != indexElt) {
                if (indexElt.isDeleted()) {
                    sendError(context, GONE, createError(DELETED, "File was deleted"));
                    return;
                }

                if (indexElt.isAuthRequired()) {
                    String authToken = MetadataParser.getAuthToken(request);
                    if (authToken == null || !indexElt.isAllowed(authToken)) {
                        log.error("You have no permission to see this file {} : token {}", indexElt.getId(), authToken);
                        sendError(context, HttpResponseStatus.FORBIDDEN, createError(ErrorCode.FORBIDDEN, "You have no permission to see this file"));
                        return;
                    }
                }

                modified = isModified(indexElt, request);

                if (modified) {
                    readerFile = fileStorage.getFile(indexElt);
                }
            } else {
                log.error("Index elt not found");
                sendError(context, HttpResponseStatus.NOT_FOUND, createError(INVALID_REQUEST, "Index elt not found"));
                return;
            }
        } catch (Exception e) {
            log.error("Unknown error", e);
            sendError(context, BAD_GATEWAY, createError(SERVICE_ERROR, "Unknown error"));
            return;
        }

        if (!modified) {
            sendNotModified(context, indexElt, request);
            return;
        }

        if (null == readerFile) {
            log.error("Can't find reader file");
            sendError(context, BAD_GATEWAY, createError(SERVICE_ERROR, "No reader file"));
            return;
        }

        if (readerFile.getLength() - indexElt.getOffset() < indexElt.getLength()) {
            String errorMessage = String.format(
                    "Reader file length less than index elt %s < %s",
                    readerFile.getLength() - indexElt.getOffset(),
                    indexElt.getLength()
            );
            log.error(errorMessage);
            sendError(context, BAD_GATEWAY, createError(SERVICE_ERROR, errorMessage));
            return;
        }

        if (FileUtils.isFileEmpty(readerFile, indexElt)) {
            String errorMessage = String.format("File %s is empty", indexElt);
            log.error(errorMessage);
            sendError(context, BAD_GATEWAY, createError(SERVICE_ERROR, errorMessage));
            return;
        }

        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);

        MetadataParser.copyMetadata(indexElt.getHeaders(), response);

        addCacheHeaders(response, indexElt);

        setContentLength(response, indexElt.getLength());

        if (isKeepAlive(request)) {
            response.headers().set(CONNECTION, KEEP_ALIVE);
        }

        context.write(response);

        // Write the content.
        context.write(
                new UnClosableFileRegion(readerFile.getFileChannel(), indexElt.getOffset(), indexElt.getLength()),
                context.voidPromise()
        );

        // Write the end marker
        ChannelFuture lastContentFuture = context.writeAndFlush(EMPTY_LAST_CONTENT);

        if (!isKeepAlive(request)) {
            lastContentFuture.addListener(CLOSE);
        }
    }

    private boolean tryHandleApiRequest(ChannelHandlerContext context, FullHttpRequest request) {
        try {
            ReturnValue<?> returnValue = requestMapper.getByUri(request.getUri()).handleRequest("{}");
            writeResponse(context.channel(), getJson(returnValue), request);
            return true;
        } catch (Exception e) {
            log.error("Can't handle request", e);
            return false;
        }
    }

    private void addWriterBack(ChannelHandlerContext ctx) {
        ctx.pipeline().remove("aggregator");
        ctx.pipeline().addBefore("reader", "writer", fileWriterHandlerProvider.get());
    }

    private void addCacheHeaders(HttpResponse response, IndexElt indexElt) {
        DateTime lastModified = new DateTime(indexElt.getCreated(), UTC);
        response.headers().add("Last-Modified", formatDate(lastModified.toDate()));
        response.headers().add("Expires", formatDate(lastModified.plusYears(EXPIRE_YEARS).toDate()));
        response.headers().add("Cache-Control", "max-age=" + (EXPIRE_YEARS * 365 * 24 * 60 * 60));
    }

    private boolean isModified(IndexElt indexElt, HttpRequest request) {
        if (request.headers().contains("if-modified-since")) {
            try {
                Date lastModified = parseDate(request.headers().get("if-modified-since"));
                DateTime lastModifiedUTC = new DateTime(lastModified, UTC);

                // drop mills from created
                long mills = new DateTime(indexElt.getCreated(), UTC).getMillisOfSecond();

                if (log.isTraceEnabled()) {
                    log.trace("File modified {}, header modified {}", indexElt.getCreated() - mills, lastModifiedUTC.getMillis());
                }

                if (indexElt.getCreated() - mills <= lastModifiedUTC.getMillis()) {
                    return false;
                }
            } catch (DateUtil.DateParseException e) {
                log.error("Can't parse date " + request.headers().get("if-modified-since"), e);
            }
        }

        return true;
    }

    private void sendNotModified(ChannelHandlerContext ctx, IndexElt indexElt, HttpRequest request) {
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, NOT_MODIFIED);

        MetadataParser.copyMetadata(indexElt.getHeaders(), response);

        response.headers().add("Last-Modified", formatDate(new DateTime(indexElt.getCreated(), UTC).toDate()));

        if (isKeepAlive(request)) {
            response.headers().set(CONNECTION, KEEP_ALIVE);
        }

        ctx.write(response);

        // Write the end marker
        ChannelFuture lastContentFuture = ctx.writeAndFlush(EMPTY_LAST_CONTENT);

        if (!isKeepAlive(request)) {
            lastContentFuture.addListener(CLOSE);
        }
    }
}
