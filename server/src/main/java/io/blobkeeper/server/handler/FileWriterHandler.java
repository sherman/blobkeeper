package io.blobkeeper.server.handler;

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

import io.blobkeeper.cluster.service.ClusterMembershipService;
import io.blobkeeper.common.domain.Result;
import io.blobkeeper.common.domain.api.ApiRequest;
import io.blobkeeper.common.domain.api.ReturnValue;
import io.blobkeeper.common.service.IdGeneratorService;
import io.blobkeeper.file.configuration.FileConfiguration;
import io.blobkeeper.file.domain.StorageFile;
import io.blobkeeper.index.domain.IndexTempElt;
import io.blobkeeper.index.service.IndexService;
import io.blobkeeper.server.handler.api.RequestHandler;
import io.blobkeeper.server.handler.api.RequestMapper;
import io.blobkeeper.server.service.UploadQueue;
import io.blobkeeper.server.util.HttpUtils;
import io.blobkeeper.server.util.MetadataParser;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.multipart.*;
import org.slf4j.Logger;

import javax.inject.Inject;
import java.io.IOException;

import static io.blobkeeper.common.domain.Error.createError;
import static io.blobkeeper.common.domain.ErrorCode.*;
import static io.blobkeeper.index.domain.IndexElt.DEFAULT_TYPE;
import static io.blobkeeper.server.util.HttpUtils.*;
import static io.netty.handler.codec.http.HttpMethod.*;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.LastHttpContent.EMPTY_LAST_CONTENT;
import static io.netty.handler.codec.http.multipart.InterfaceHttpData.HttpDataType.FileUpload;
import static java.lang.String.format;
import static org.joda.time.DateTime.now;
import static org.joda.time.DateTimeZone.UTC;
import static org.slf4j.LoggerFactory.getLogger;

public class FileWriterHandler extends BaseFileHandler<HttpObject> {
    private static final Logger log = getLogger(FileWriterHandler.class);

    private static final int DEFAULT_SHARD_ID = 1;

    @Inject
    private IdGeneratorService isGeneratorService;

    @Inject
    private IndexService indexService;

    @Inject
    private UploadQueue uploadQueue;

    @Inject
    private ClusterMembershipService clusterMembershipService;

    @Inject
    private RequestMapper requestMapper;

    @Inject
    private FileConfiguration fileConfiguration;

    private HttpRequest request;

    private HttpCustomPostRequestDecoder decoder;

    private boolean requestIsSent = false;
    private boolean errorRequest = false;

    // clean up garbage
    static {
        DiskFileUpload.deleteOnExitTemporaryFile = false;
        DiskAttribute.deleteOnExitTemporaryFile = true;
    }

    // set up base upload directory
    public void init() {
        DiskFileUpload.baseDirectory = fileConfiguration.getUploadPath();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.warn("Unknown exception", cause);
        ctx.channel().close();
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        if (decoder != null) {
            reset();
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (decoder != null) {
            errorRequest = true;
            reset();
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext context, HttpObject object) throws Exception {
        setContext();

        log.debug("{}", object);

        if (object instanceof HttpRequest) {
            HttpRequest request = (HttpRequest) object;

            if (request.getMethod() == GET) {
                jumpToReader(context, object);
                return;
            }

            if (request.getMethod() == DELETE) {
                jumpToDeleter(context, object);
                return;
            }

            if (request.getMethod() == PUT) {
                // FIXME: restore will be here
                return;
            }
        }

        if (object.equals(EMPTY_LAST_CONTENT)) {
            String errorMessage = "There is no upload file in the request";
            log.error(errorMessage);
            sendError(context, BAD_REQUEST, createError(INVALID_REQUEST, errorMessage));
            return;
        }

        createPostDecoderIfNotExists(context, object);

        if (null == decoder) {
            // FIXME: send error
            return;
        }

        if (object instanceof HttpContent) {
            // new chunk is received
            HttpContent chunk = (HttpContent) object;
            try {
                decoder.offer(chunk);
            } catch (HttpPostRequestDecoder.ErrorDataDecoderException e1) {
                log.error("Can't decode chunk", e1);

                sendError(context, BAD_REQUEST, createError(INVALID_REQUEST, "Can't decode chunk"));
                return;
            }

            readHttpDataChunkByChunk(context);

            if (chunk instanceof LastHttpContent) {
                if (!requestIsSent) {
                    String errorMessage = "There is no upload file in the request";
                    log.error(errorMessage);
                    sendError(context, BAD_REQUEST, createError(INVALID_REQUEST, errorMessage));
                }
                reset();
            }
        }
    }

    @Override
    protected void sendError(ChannelHandlerContext ctx, HttpResponseStatus status, io.blobkeeper.common.domain.Error error) {
        this.errorRequest = true;
        super.sendError(ctx, status, error);
    }

    private void handleApiRequest(ChannelHandlerContext ctx, String value) {
        try {
            RequestHandler<?, ? extends ApiRequest> requestHandler = requestMapper.getByUri(this.request.getUri());
            ReturnValue<?> returnValue = requestHandler.handleRequest(value);
            writeResponse(ctx, returnValue, request);
        } catch (Exception e) {
            log.error("Can't handle request", e);
        }
    }

    private void jumpToDeleter(ChannelHandlerContext context, HttpObject object) {
        context.pipeline().addBefore("deleter", "aggregator", new HttpObjectAggregator(65536));
        context.pipeline().remove(FileWriterHandler.class);
        context.fireChannelRead(object);
    }

    private void jumpToReader(ChannelHandlerContext context, HttpObject object) {
        context.pipeline().addBefore("reader", "aggregator", new HttpObjectAggregator(65536));
        context.pipeline().remove(this);
        context.fireChannelRead(object);
    }

    private void createPostDecoderIfNotExists(ChannelHandlerContext ctx, HttpObject request) {
        if (decoder == null) {
            try {
                this.request = (HttpRequest) request;
                // always save data on disk
                CustomHttpDataFactory dataFactory = new CustomHttpDataFactory();
                decoder = new HttpCustomPostRequestDecoder(dataFactory, this.request);
            } catch (Exception e) {
                log.error("Can't decode message", e);
                sendError(ctx, BAD_REQUEST, createError(INVALID_REQUEST, "Can't decode request"));
            }
        }
    }

    private void reset() {
        log.debug("Release all resources from decoder started");

        if (errorRequest) {
            cleanFiles();
        }

        request = null;
        decoder.destroy();
        decoder = null;

        log.debug("Release all resources from decoder completed");
    }

    private void cleanFiles() {
        decoder.cleanFilesOnError();
    }

    private void readHttpDataChunkByChunk(ChannelHandlerContext ctx) {
        try {
            while (decoder.hasNext()) {
                InterfaceHttpData data = decoder.next();
                if (data != null) {
                    // new value
                    writeHttpData(ctx, data);
                }
            }
        } catch (HttpPostRequestDecoder.EndOfDataDecoderException e1) {
            // FIXME: send error?
            log.debug("End of a request!");
        }
    }

    private void writeHttpData(ChannelHandlerContext ctx, InterfaceHttpData data) {
        if (data.getHttpDataType() == InterfaceHttpData.HttpDataType.Attribute) {
            Attribute attribute = (Attribute) data;
            String value;
            try {
                value = attribute.getValue();
                log.info("Api call: {}", value);

                handleApiRequest(ctx, value);
                requestIsSent = true;
                return;
            } catch (IOException e) {
                log.error(format("Can't read data, attribute name is %s", attribute.getHttpDataType().name()), e);

                sendError(ctx, BAD_REQUEST, createError(INVALID_REQUEST, "Can't read data"));
                return;
            }
        } else {
            if (data.getHttpDataType() == FileUpload) {
                requestIsSent = true;

                if (!clusterMembershipService.isMaster()) {
                    log.error("Node is not a master");
                    sendError(ctx, METHOD_NOT_ALLOWED, createError(NOT_A_MASTER, "Node is not a master"));
                    return;
                }

                String uri = request.getUri();
                long id = getId(uri);
                int type = getType(uri);

                FileUpload fileUpload = (FileUpload) data;
                if (fileUpload.isCompleted()) {
                    if (id == HttpUtils.NOT_FOUND || type == HttpUtils.NOT_FOUND) {
                        id = isGeneratorService.generate(DEFAULT_SHARD_ID);
                        type = DEFAULT_TYPE;
                        log.info("New id : type is {} : {}", id, type);
                    } else {
                        log.info("Given file : type is {} : {}", id, type);

                        if (null != indexService.getById(id, type)) {
                            log.error("File {} : {} is already present");
                            sendError(ctx, CONFLICT, createError(ALREADY_EXISTS, "Object already exists"));
                            return;
                        }
                    }

                    log.trace("Started copying a file to the write queue");

                    StorageFile storageFile = buildStorageFile(fileUpload)
                            .id(id)
                            .type(type)
                            .headers(MetadataParser.getHeaders(request))
                            .build();

                    addTempIndex(storageFile);

                    // add file to the upload queue
                    if (!uploadQueue.offer(storageFile)) {
                        String errorMessage = "Upload failed";
                        log.error(errorMessage);
                        sendError(ctx, BAD_GATEWAY, createError(SERVICE_ERROR, errorMessage));
                    } else {
                        log.info("File {} added to the upload queue", id);
                        writeResponse(ctx, new ReturnValue<>(new Result(id)), request);
                        return;
                    }
                } else {
                    String errorMessage = "Upload file to be continued but should not!";
                    log.error(errorMessage);
                    sendError(ctx, BAD_REQUEST, createError(INVALID_REQUEST, errorMessage));
                }
            }
        }

        if (!requestIsSent) {
            String errorMessage = "There is no upload file in the request";
            log.error(errorMessage);
            sendError(ctx, BAD_REQUEST, createError(INVALID_REQUEST, errorMessage));
        }
    }

    /**
     * Save a temp index for recovering
     */
    private void addTempIndex(StorageFile storageFile) {
        IndexTempElt indexElt = new IndexTempElt.IndexTempEltBuilder()
                .id(storageFile.getId())
                .type(storageFile.getType())
                .created(now(UTC).getMillis())
                .metadata(storageFile.getMetadata())
                .file(storageFile.getFile().getAbsolutePath())
                .build();

        indexService.add(indexElt);
    }
}
