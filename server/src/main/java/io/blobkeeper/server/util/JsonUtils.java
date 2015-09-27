package io.blobkeeper.server.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.blobkeeper.common.domain.Error;
import io.blobkeeper.common.domain.ErrorCode;
import io.blobkeeper.common.domain.Result;
import io.blobkeeper.common.domain.api.ResultValue;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;

import static org.slf4j.LoggerFactory.getLogger;

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

@Singleton
public class JsonUtils {
    private static final Logger log = getLogger(JsonUtils.class);

    @Inject
    private ObjectMapper objectMapper;

    public String toJson(@NotNull Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            log.error("Can't encode json", e);
            try {
                return objectMapper.writeValueAsString(Error.createError(ErrorCode.SERVICE_ERROR, "Unknown error"));
            } catch (JsonProcessingException e1) {/* bad luck */}
        }

        throw new RuntimeException("Unreachable!");
    }

    public Result getFromJson(@NotNull String json) {
        try {
            @SuppressWarnings("unchecked")
            ResultValue returnValue = objectMapper.readValue(json, ResultValue.class);
            return returnValue.getResult();
        } catch (IOException e) {
            throw new IllegalArgumentException("Can't parse Result object", e);
        }
    }
}
