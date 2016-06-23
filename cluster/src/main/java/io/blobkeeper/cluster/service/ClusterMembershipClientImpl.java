package io.blobkeeper.cluster.service;

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

import io.blobkeeper.cluster.domain.Node;
import io.blobkeeper.cluster.domain.CustomMessageHeader;
import io.blobkeeper.cluster.domain.ReplicationServiceException;
import io.blobkeeper.index.domain.CacheKey;
import org.jetbrains.annotations.NotNull;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.blobkeeper.cluster.domain.Command.CACHE_INVALIDATE_REQUEST;
import static io.blobkeeper.cluster.util.ClusterUtils.createMessage;
import static java.util.concurrent.CompletableFuture.runAsync;

@Singleton
public class ClusterMembershipClientImpl implements ClusterMembershipClient {
    private static final Logger log = LoggerFactory.getLogger(ClusterMembershipClientImpl.class);

    @Inject
    private ClusterMembershipService membershipService;

    @Override
    public void invalidateCache(@NotNull CacheKey cacheKey) {
        if (log.isTraceEnabled()) {
            log.trace("Invalidate cache for {}", cacheKey);
        }

        Optional<Node> masterNode = membershipService.getMaster();

        masterNode.ifPresent(
                master -> membershipService.getNodes()
                        .stream()
                        .filter(node -> !(node.equals(master) || node.equals(membershipService.getSelfNode())))
                        .forEach(node -> runAsync(() -> invalidateCache(cacheKey, node.getAddress())))
        );
    }


    private void invalidateCache(CacheKey cacheKey, Address dst) {
        JChannel channel = membershipService.getChannel();
        log.trace("Invalidate cache packet sending for {}", dst);
        try {
            Message message = createMessage(
                    membershipService.getSelfNode().getAddress(),
                    dst,
                    cacheKey,
                    new CustomMessageHeader(CACHE_INVALIDATE_REQUEST)
            );

            channel.send(message);
        } catch (Exception e) {
            log.error("Can't replicate file", e);
            throw new ReplicationServiceException(e);
        }
    }
}
