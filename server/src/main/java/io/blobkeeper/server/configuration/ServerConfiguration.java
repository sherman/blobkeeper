package io.blobkeeper.server.configuration;

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
import com.google.inject.Provides;
import org.jetbrains.annotations.TestOnly;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.net.MalformedURLException;
import java.net.URL;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.ANY;
import static com.fasterxml.jackson.annotation.PropertyAccessor.FIELD;
import static com.google.common.base.Splitter.on;
import static com.google.common.collect.FluentIterable.from;

@Singleton
public class ServerConfiguration {

    @Inject
    @Named("blobkeeper.server.name")
    private String serverName;

    @Inject
    @Named("blobkeeper.server.host")
    private String serverHost;

    @Inject
    @Named("blobkeeper.server.port")
    private int serverPort;

    @Inject
    @Named("blobkeeper.server.allowed.headers")
    private String allowedHeaders;

    @Inject
    @Named("blobkeeper.server.api.token")
    private String apiToken;

    @Inject
    @Named("blobkeeper.server.secret.token")
    private String secretToken;

    private long writerTaskStartDelay;

    private volatile String[] allowedHeadersCache;

    @Provides
    @Singleton
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setVisibility(FIELD, ANY);
        return mapper;
    }

    public int getServerPort() {
        return serverPort;
    }

    public String getServerName() {
        return serverName;
    }

    public String getServerHost() {
        return serverHost;
    }

    public URL getBaseUrl() {
        try {
            return new URL("http://" + getServerHost() + ":" + getServerPort());
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    public String[] getAllowedHeaders() {
        if (null == allowedHeadersCache) {
            allowedHeadersCache = from(on(",").split(allowedHeaders)).toArray(String.class);
        }

        return allowedHeadersCache;
    }

    public long getWriterTaskStartDelay() {
        return writerTaskStartDelay;
    }

    @TestOnly
    public void setWriterTaskStartDelay(long writerTaskStartDelay) {
        this.writerTaskStartDelay = writerTaskStartDelay;
    }

    public String getApiToken() {
        return apiToken;
    }

    public String getSecretToken() {
        return secretToken;
    }
}
