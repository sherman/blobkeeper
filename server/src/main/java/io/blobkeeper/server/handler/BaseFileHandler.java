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

import com.google.common.collect.ImmutableMap;
import io.blobkeeper.cluster.service.ClusterMembershipService;
import io.blobkeeper.common.domain.api.ReturnValue;
import io.blobkeeper.common.logging.MdcContext;
import io.blobkeeper.server.util.ErrorCodeResolver;
import io.blobkeeper.server.util.JsonUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import org.jetbrains.annotations.NotNull;

import javax.inject.Inject;

import static io.blobkeeper.common.logging.MdcContext.SRC_NODE;
import static io.blobkeeper.common.util.MdcUtils.setCurrentContext;
import static io.netty.buffer.Unpooled.copiedBuffer;
import static io.netty.channel.ChannelFutureListener.CLOSE;
import static io.netty.handler.codec.http.HttpHeaders.Names.*;
import static io.netty.handler.codec.http.HttpHeaders.Values.KEEP_ALIVE;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_0;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static io.netty.util.CharsetUtil.UTF_8;

public abstract class BaseFileHandler<T> extends SimpleChannelInboundHandler<T> {

    @Inject
    private JsonUtils jsonUtils;

    @Inject
    private ErrorCodeResolver errorCodeResolver;

    @Inject
    protected ClusterMembershipService clusterMembershipService;

    protected void writeResponse(Channel channel, String result, HttpRequest request) {
        // Convert the response content to a ChannelBuffer.
        ByteBuf buf = copiedBuffer(result, CharsetUtil.UTF_8);

        // Decide whether to close the connection or not.
        boolean close = HttpHeaders.Values.CLOSE.equalsIgnoreCase(request.headers().get(CONNECTION))
                || request.getProtocolVersion().equals(HTTP_1_0)
                && !KEEP_ALIVE.equalsIgnoreCase(request.headers().get(CONNECTION));

        // Build the response object.
        FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK, buf);
        response.headers().set(CONTENT_TYPE, "application/json; charset=UTF-8");

        if (!close) {
            // There's no need to add 'Content-Length' header
            // if this is the last response.
            response.headers().set(CONTENT_LENGTH, buf.readableBytes());
        }

        // Write the response.
        ChannelFuture future = channel.writeAndFlush(response);
        // Close the connection after the write operation is done if necessary.
        if (close) {
            future.addListener(ChannelFutureListener.CLOSE);
        }
    }

    protected void sendError(ChannelHandlerContext ctx, HttpResponseStatus status, io.blobkeeper.common.domain.Error error) {
        sendError(ctx, status, new ReturnValue<>(error));
    }

    protected void sendError(ChannelHandlerContext ctx, HttpResponseStatus status, ReturnValue<?> returnValue) {
        FullHttpResponse response = new DefaultFullHttpResponse(
                HTTP_1_1,
                status,
                Unpooled.copiedBuffer(getJson(returnValue), UTF_8)
        );
        response.headers().set(CONTENT_TYPE, "application/json; charset=UTF-8");

        // Close the connection as soon as the error message is sent.
        ctx.writeAndFlush(response).addListener(CLOSE);
    }

    protected void writeResponse(ChannelHandlerContext ctx, ReturnValue<?> returnValue, HttpRequest request) {
        if (returnValue.hasError()) {
            HttpResponseStatus responseStatus = errorCodeResolver.getResponseStatus(returnValue.getError().getCode());
            sendError(ctx, responseStatus, returnValue);
        } else {
            writeResponse(ctx.channel(), getJson(returnValue), request);
        }
    }

    protected String getJson(@NotNull Object responseContent) {
        return jsonUtils.toJson(responseContent);
    }

    protected void setContext() {
        setCurrentContext(new MdcContext(ImmutableMap.of(SRC_NODE, clusterMembershipService.getSelfNode().toString())));
    }
}
