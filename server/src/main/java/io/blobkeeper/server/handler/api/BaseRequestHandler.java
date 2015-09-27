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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.blobkeeper.common.domain.api.ApiRequest;
import io.blobkeeper.common.domain.api.ReturnValue;
import io.blobkeeper.server.handler.api.authentication.ApiRequestAuthenticationService;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;

import static io.blobkeeper.common.domain.Error.createError;
import static io.blobkeeper.common.domain.ErrorCode.INVALID_REQUEST;

public abstract class BaseRequestHandler<T, R extends ApiRequest> implements RequestHandler<T, R> {
    private static final Logger log = LoggerFactory.getLogger(BaseRequestHandler.class);

    @Inject
    private ObjectMapper objectMapper;

    @Inject
    private ApiRequestAuthenticationService authenticationService;

    protected BaseRequestHandler() {
    }

    protected abstract ReturnValue<T> handlerRequest(@NotNull R request);

    protected abstract Class<? extends R> getRequestClass();

    @Override
    public ReturnValue<T> handleRequest(@NotNull String data) {
        R request;
        try {
            request = getRequest(data);
        } catch (Exception e) {
            log.error("Can't get data", e);
            return new ReturnValue<>(createError(INVALID_REQUEST, "Invalid request"));
        }

        if (!authenticationService.isAuthenticated(request)) {
            return new ReturnValue<>(createError(INVALID_REQUEST, "Invalid token"));
        }

        return handlerRequest(request);
    }

    protected R getRequest(@NotNull String data) {
        try {
            return objectMapper.readValue(data, getRequestClass());
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
