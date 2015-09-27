package io.blobkeeper.server.initializer;

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

import io.blobkeeper.server.configuration.ServerConfiguration;
import io.blobkeeper.server.handler.FileDeleteHandler;
import io.blobkeeper.server.handler.FileReaderHandler;
import io.blobkeeper.server.handler.FileWriterHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.cors.CorsConfig;
import io.netty.handler.codec.http.cors.CorsHandler;
import io.netty.handler.stream.ChunkedWriteHandler;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import static io.netty.handler.codec.http.HttpMethod.*;
import static io.netty.handler.codec.http.HttpMethod.PUT;

@Singleton
public class BlobKeeperServerInitializer extends ChannelInitializer<SocketChannel> {

    @Inject
    private ServerConfiguration serverConfiguration;

    @Inject
    private Provider<FileWriterHandler> fileWriterHandlerProvider;

    @Inject
    private FileReaderHandler fileReaderHandler;

    @Inject
    private FileDeleteHandler fileDeleteHandler;

    @Override
    protected void initChannel(SocketChannel socketChannel) throws Exception {
        CorsConfig corsConfig = CorsConfig
                .withAnyOrigin()
                .allowedRequestMethods(DELETE, GET, OPTIONS, POST, PUT)
                .allowedRequestHeaders(serverConfiguration.getAllowedHeaders())
                .build();

        ChannelPipeline pipeline = socketChannel.pipeline();
        pipeline.addLast("decoder", new HttpRequestDecoder());
        pipeline.addLast("encoder", new HttpResponseEncoder());
        pipeline.addLast("chunkedWriter", new ChunkedWriteHandler());
        pipeline.addLast("cors", new CorsHandler(corsConfig));

        pipeline.addLast("writer", fileWriterHandlerProvider.get());
        pipeline.addLast("reader", fileReaderHandler);
        pipeline.addLast("deleter", fileDeleteHandler);
    }
}
