package io.blobkeeper.server.handler.api.master;

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
import io.blobkeeper.cluster.service.ClusterMembershipService;
import io.blobkeeper.common.domain.api.MasterNode;
import io.blobkeeper.common.domain.api.ReturnValue;
import io.blobkeeper.common.domain.api.SetMasterApiRequest;
import io.blobkeeper.server.handler.api.BaseRequestHandler;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;

import static io.blobkeeper.common.domain.Error.createError;
import static io.blobkeeper.common.domain.ErrorCode.NODE_IS_NOT_EXISTS;
import static io.blobkeeper.common.domain.ErrorCode.SERVICE_ERROR;

@Singleton
public class SetMasterHandler extends BaseRequestHandler<Boolean, SetMasterApiRequest> {
    private static final Logger log = LoggerFactory.getLogger(SetMasterHandler.class);

    @Inject
    private ClusterMembershipService clusterMembershipService;

    @Override
    protected ReturnValue<Boolean> handlerRequest(@NotNull SetMasterApiRequest request) {
        log.info("Set new master");

        try {
            Node master = getMaster(request.getNode());
            if (!clusterMembershipService.trySetMaster(master.getAddress())) {
                return new ReturnValue<>(createError(SERVICE_ERROR, "Can't set new master"));
            } else {
                return new ReturnValue<>(true);
            }
        } catch (Exception e) {
            log.error("Can't find node with given address {}", request.getNode(), e);
            return new ReturnValue<>(createError(NODE_IS_NOT_EXISTS, "Node is not exists!"));
        }
    }

    @Override
    protected Class<? extends SetMasterApiRequest> getRequestClass() {
        return SetMasterApiRequest.class;
    }

    private Node getMaster(MasterNode masterNode) {
        return clusterMembershipService.getNodes()
                .stream()
                .filter(node -> node.getAddress().toString().equals(masterNode.getAddress()))
                .findFirst()
                .get();
    }
}
