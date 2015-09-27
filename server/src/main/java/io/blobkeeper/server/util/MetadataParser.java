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

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Splitter.on;
import static com.google.common.collect.FluentIterable.from;
import static io.blobkeeper.common.util.MetadataUtils.AUTH_TOKEN_HEADER;
import static io.blobkeeper.common.util.MetadataUtils.CONTENT_TYPE_HEADER;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;

public class MetadataParser {
    private static final Logger log = LoggerFactory.getLogger(MetadataParser.class);

    private MetadataParser() {
    }

    public static Multimap<String, String> getHeaders(@NotNull HttpRequest request) {
        checkNotNull(request, "request is required!");

        Iterable<Map.Entry<String, String>> filtered = from(request.headers()).filter(
                elt -> elt.getKey().startsWith("X-Metadata")
        );

        ImmutableMultimap.Builder<String, String> valueBuilder = ImmutableMultimap.builder();

        for (Map.Entry<String, String> elt : filtered) {
            if (elt.getKey().equals(AUTH_TOKEN_HEADER)) {
                for (String accessToken : from(on(",").split(elt.getValue()))) {
                    valueBuilder.put(elt.getKey(), accessToken);
                }
            } else {
                valueBuilder.put(elt.getKey(), elt.getValue());
            }
        }

        return valueBuilder.build();
    }

    public static void copyMetadata(@NotNull Multimap<String, String> headers, @NotNull HttpResponse response) {
        HttpHeaders httpHeaders = response.headers();
        for (Map.Entry<String, String> elt : headers.entries()) {
            if (elt.getKey().equals(AUTH_TOKEN_HEADER)) {
                continue;
            }

            if (elt.getKey().equals(CONTENT_TYPE_HEADER)) {
                httpHeaders.add(CONTENT_TYPE, elt.getValue());
            } else {
                httpHeaders.add(elt.getKey(), elt.getValue());
            }
        }
    }

    public static String getAuthToken(@NotNull HttpRequest request) {
        return request.headers().get(AUTH_TOKEN_HEADER);
    }
}
