package io.blobkeeper.server.handler.api;

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

import io.blobkeeper.cluster.service.ClusterMembershipClient;
import io.blobkeeper.common.domain.Result;
import io.blobkeeper.common.domain.api.FileRequest;
import io.blobkeeper.common.domain.api.ReturnValue;
import io.blobkeeper.index.configuration.IndexConfiguration;
import io.blobkeeper.index.domain.IndexElt;
import io.blobkeeper.index.service.IndexService;
import io.blobkeeper.server.util.HttpUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Singleton;

import static io.blobkeeper.common.domain.Error.createError;
import static io.blobkeeper.common.domain.ErrorCode.INVALID_REQUEST;
import static io.blobkeeper.common.domain.ErrorCode.SERVICE_ERROR;
import static io.blobkeeper.index.domain.IndexElt.DEFAULT_TYPE;
import static io.blobkeeper.server.util.HttpUtils.getApiToken;
import static io.blobkeeper.server.util.HttpUtils.getId;
import static org.slf4j.LoggerFactory.getLogger;

@Singleton
public class DeleteRequestHandler extends BaseRequestHandler<Result, FileRequest> {
    private static final Logger log = getLogger(DeleteRequestHandler.class);

    @Inject
    private IndexService indexService;

    @Inject
    private ClusterMembershipClient membershipClient;

    @Inject
    private IndexConfiguration indexConfiguration;

    @Override
    protected ReturnValue<Result> handlerRequest(@NotNull FileRequest request) {
        if (HttpUtils.NOT_FOUND == request.getId()) {
            log.error("No file id");
            return new ReturnValue<>(createError(INVALID_REQUEST, "No id"));
        }

        IndexElt indexElt;
        try {
            log.debug("Id {}", request.getId());
            indexElt = indexService.getById(request.getId(), DEFAULT_TYPE);
            log.debug("Index elt is {}", indexElt);
            if (null != indexElt) {
                // update index, mark that original file and all thumbs were deleted
                indexService.delete(indexElt);

                if (indexConfiguration.isCacheEnabled()) {
                    membershipClient.invalidateCache(indexElt.toCacheKey());
                }
            } else {
                log.error("Index elt not found");
                return new ReturnValue<>(createError(INVALID_REQUEST, "Index elt not found"));
            }
        } catch (Exception e) {
            log.error("Unknown error", e);
            return new ReturnValue<>(createError(SERVICE_ERROR, "Unknown error"));
        }

        return new ReturnValue<>(new Result(indexElt.getId()));
    }

    @Override
    protected Class<? extends FileRequest> getRequestClass() {
        return FileRequest.class;
    }

    @Override
    protected FileRequest getRequest(@NotNull String data) {
        long id = getId(data);
        String apiToken = getApiToken(data);

        FileRequest request = new FileRequest();
        request.setId(id);
        request.setToken(apiToken);

        return request;
    }
}
