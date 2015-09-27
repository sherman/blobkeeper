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


import io.blobkeeper.common.domain.Result;
import io.blobkeeper.common.domain.api.ReturnValue;
import io.blobkeeper.server.handler.api.DeleteRequestHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import static io.blobkeeper.common.domain.Error.createError;
import static io.blobkeeper.common.domain.ErrorCode.INVALID_REQUEST;
import static io.netty.handler.codec.http.HttpMethod.DELETE;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpResponseStatus.*;

@Singleton
@ChannelHandler.Sharable
public class FileDeleteHandler extends BaseFileHandler<FullHttpRequest> {
    private static final Logger log = LoggerFactory.getLogger(FileDeleteHandler.class);

    @Inject
    private Provider<FileWriterHandler> fileWriterHandlerProvider;

    @Inject
    private DeleteRequestHandler requestHandler;

    @Override
    protected void channelRead0(ChannelHandlerContext context, FullHttpRequest request) throws Exception {
        setContext();

        if (request.getMethod() == POST) {
            context.fireChannelRead(request.copy());
            return;
        }

        addWriterBack(context);

        if (log.isTraceEnabled()) {
            log.trace("Request is: {}", request);
        }

        if (request.getUri().equals("/favicon.ico")) {
            sendError(context, NOT_FOUND, createError(INVALID_REQUEST, "No favorite icon here"));
            return;
        }

        if (!request.getDecoderResult().isSuccess()) {
            sendError(context, BAD_REQUEST, createError(INVALID_REQUEST, "Strange request given"));
            return;
        }

        if (request.getMethod() != DELETE) {
            sendError(context, METHOD_NOT_ALLOWED, createError(INVALID_REQUEST, "Only DELETE requests are acceptable"));
            return;
        }

        handleApiRequest(context, request);
    }

    private void addWriterBack(ChannelHandlerContext ctx) {
        ctx.pipeline().remove("aggregator");
        ctx.pipeline().addBefore("reader", "writer", fileWriterHandlerProvider.get());
    }

    private void handleApiRequest(ChannelHandlerContext context, FullHttpRequest request) {
        try {
            ReturnValue<Result> returnValue = requestHandler.handleRequest(request.getUri());
            writeResponse(context, returnValue, request);
        } catch (Exception e) {
            log.error("Can't handle request", e);
        }
    }
}
