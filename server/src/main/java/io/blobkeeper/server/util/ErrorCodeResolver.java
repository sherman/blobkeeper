package io.blobkeeper.server.util;

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
import io.blobkeeper.common.domain.ErrorCode;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.util.Map;

import static io.netty.handler.codec.http.HttpResponseStatus.*;

@Singleton
public class ErrorCodeResolver {
    private static final Logger log = LoggerFactory.getLogger(ErrorCodeResolver.class);

    private final Map<ErrorCode, HttpResponseStatus> errorsToHttpCodes = ImmutableMap.of(
            ErrorCode.DELETED, GONE,
            ErrorCode.SERVICE_ERROR, BAD_GATEWAY,
            ErrorCode.INVALID_REQUEST, BAD_REQUEST,
            ErrorCode.NOT_A_MASTER, METHOD_NOT_ALLOWED,
            ErrorCode.NODE_IS_NOT_EXISTS, BAD_REQUEST
    );

    @NotNull
    public HttpResponseStatus getResponseStatus(@NotNull ErrorCode errorCode) {
        HttpResponseStatus responseStatus = errorsToHttpCodes.get(errorCode);
        if (null == responseStatus) {
            log.error("Can't find http status for " + errorCode);
            return BAD_REQUEST;
        } else {
            return responseStatus;
        }
    }
}
