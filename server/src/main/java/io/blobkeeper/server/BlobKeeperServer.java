package io.blobkeeper.server;

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

import com.google.common.util.concurrent.AbstractService;
import io.blobkeeper.cluster.service.ClusterMembershipService;
import io.blobkeeper.file.service.FileStorage;
import io.blobkeeper.server.configuration.ServerConfiguration;
import io.blobkeeper.server.initializer.BlobKeeperServerInitializer;
import io.blobkeeper.server.service.FileWriterService;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;

import javax.inject.Inject;
import javax.inject.Singleton;

import static io.netty.channel.ChannelOption.*;
import static io.netty.channel.ChannelOption.ALLOCATOR;

@Singleton
public class BlobKeeperServer extends AbstractService {

    @Inject
    private BlobKeeperServerInitializer serverInitializer;

    @Inject
    private ServerConfiguration serverConfiguration;

    @Inject
    private FileWriterService fileWriterService;

    @Inject
    private ClusterMembershipService clusterMembershipService;

    private ServerBootstrap bootstrap;
    private ChannelFuture serverChannel;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    @Override
    protected void doStart() {
        fileWriterService.start();
        clusterMembershipService.start(serverConfiguration.getServerName());

        bossGroup = new EpollEventLoopGroup();
        // FIXME: add to config
        workerGroup = new EpollEventLoopGroup(512);

        bootstrap = new ServerBootstrap();
        bootstrap.option(ALLOCATOR, PooledByteBufAllocator.DEFAULT);

        bootstrap.group(bossGroup, workerGroup)
                .channel(EpollServerSocketChannel.class)
                .childHandler(serverInitializer);

        bootstrap.childOption(SO_LINGER, -1);
        bootstrap.childOption(TCP_NODELAY, true);
        bootstrap.childOption(SO_REUSEADDR, true);
        bootstrap.childOption(ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        bootstrap.childOption(EpollChannelOption.SO_REUSEPORT, true);
        bootstrap.childOption(EpollChannelOption.TCP_CORK, true);

        try {
            serverChannel = bootstrap.bind(serverConfiguration.getServerPort()).sync();
            notifyStarted();
        } catch (InterruptedException e) {
            notifyFailed(e);
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void doStop() {
        if (null != serverChannel) {
            // close server channel and incoming connections
            serverChannel.channel().close();
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();

            fileWriterService.stop();

            serverChannel = null;

            clusterMembershipService.stop();
        }

        notifyStopped();
    }
}
