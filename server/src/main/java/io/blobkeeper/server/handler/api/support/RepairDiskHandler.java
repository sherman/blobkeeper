package io.blobkeeper.server.handler.api.support;

/*
 * Copyright (C) 2016 by Denis M. Gabaydulin
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

import io.blobkeeper.cluster.service.RepairService;
import io.blobkeeper.common.domain.api.RepairDiskRequest;
import io.blobkeeper.common.domain.api.ReturnValue;
import io.blobkeeper.server.handler.api.BaseRequestHandler;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;

import static io.blobkeeper.common.domain.Error.createError;
import static io.blobkeeper.common.domain.ErrorCode.SERVICE_ERROR;

@Singleton
public class RepairDiskHandler extends BaseRequestHandler<Boolean, RepairDiskRequest> {
    private static final Logger log = LoggerFactory.getLogger(RepairDiskHandler.class);

    @Inject
    private RepairService repairService;

    @Override
    protected ReturnValue<Boolean> handlerRequest(@NotNull RepairDiskRequest request) {
        log.info("Repair disks started");

        try {
            repairService.repair(true);
            return new ReturnValue<>(true);
        } catch (Exception e) {
            log.error("Can't repair disks", e);
            return new ReturnValue<>(createError(SERVICE_ERROR, "Can't repair disks!"));
        } finally {
            log.info("Repair disks finished");
        }
    }

    @Override
    protected Class<? extends RepairDiskRequest> getRequestClass() {
        return RepairDiskRequest.class;
    }
}
