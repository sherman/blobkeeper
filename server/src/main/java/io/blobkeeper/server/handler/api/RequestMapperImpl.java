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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import io.blobkeeper.common.domain.api.ApiRequest;
import io.blobkeeper.common.domain.api.UriType;
import io.blobkeeper.server.handler.api.master.IsMasterHandler;
import io.blobkeeper.server.handler.api.master.RemoveMasterHandler;
import io.blobkeeper.server.handler.api.master.SetMasterHandler;
import org.jetbrains.annotations.NotNull;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Map;

import static io.blobkeeper.common.domain.api.UriType.*;

@Singleton
public class RequestMapperImpl implements RequestMapper {

    @Inject
    private Injector injector;

    private Map<UriType, Class<? extends RequestHandler<?, ? extends ApiRequest>>> handlers =
            ImmutableMap.<UriType, Class<? extends RequestHandler<?, ? extends ApiRequest>>>of(
                    MASTER, IsMasterHandler.class,
                    SET_MASTER, SetMasterHandler.class,
                    REMOVE_MASTER, RemoveMasterHandler.class
            );

    @Override
    public RequestHandler<?, ? extends ApiRequest> getByUri(@NotNull String uri) {
        UriType uriType = UriType.fromUri(uri);
        if (null == uriType) {
            throw new RuntimeException("Can't find uri");
        }

        Class<? extends RequestHandler<?, ? extends ApiRequest>> handlerClass = handlers.get(uriType);
        if (null == handlerClass) {
            throw new RuntimeException("Can't find handler by uri " + uriType);
        }

        return injector.getInstance(handlerClass);
    }
}
