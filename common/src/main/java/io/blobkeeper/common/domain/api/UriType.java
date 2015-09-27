package io.blobkeeper.common.domain.api;

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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.function.Function;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toMap;

public enum UriType {
    MASTER("/master"),
    SET_MASTER("/setMaster"),
    REMOVE_MASTER("/removeMaster"),
    REPAIR("/repair"),
    REFRESH("/refresh");

    private String uri;

    UriType(String uri) {
        this.uri = uri;
    }

    public String getUri() {
        return uri;
    }

    private static final Map<String, UriType> uriLookUp = stream(UriType.values())
            .collect(toMap(UriType::getUri, Function.<UriType>identity()));

    @Nullable
    public static UriType fromUri(@NotNull String uri) {
        return uriLookUp.get(uri);
    }
}
