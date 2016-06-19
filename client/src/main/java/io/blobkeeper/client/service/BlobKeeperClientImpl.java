package io.blobkeeper.client.service;

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
import com.google.common.util.concurrent.AbstractService;
import io.blobkeeper.common.domain.api.*;
import org.asynchttpclient.*;
import org.asynchttpclient.channel.DefaultKeepAliveStrategy;
import org.asynchttpclient.request.body.multipart.FilePart;
import org.asynchttpclient.request.body.multipart.StringPart;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkArgument;
import static io.blobkeeper.common.util.MetadataUtils.AUTH_TOKEN_HEADER;

public class BlobKeeperClientImpl extends AbstractService implements BlobKeeperClient {
    private static final Logger log = LoggerFactory.getLogger(BlobKeeperClientImpl.class);

    private final ObjectMapper objectMapper;
    private final URL baseUrl;
    private final AsyncHttpClient httpClient;

    public BlobKeeperClientImpl(ObjectMapper objectMapper, URL baseUrl, AsyncHttpClientConfig config) {
        this.objectMapper = objectMapper;
        this.baseUrl = baseUrl;
        httpClient = new DefaultAsyncHttpClient(config);
    }

    public BlobKeeperClientImpl(ObjectMapper objectMapper, URL baseUrl) {
        this(objectMapper, baseUrl, new DefaultAsyncHttpClientConfig.Builder()
                .setFollowRedirect(true)
                .setKeepAlive(true)
                .setKeepAliveStrategy(new DefaultKeepAliveStrategy())
                .setConnectionTtl(5000)
                .setRequestTimeout(5000)
                .setMaxRequestRetry(3)
                .setMaxConnections(8192)
                .setSoLinger(-1)
                .setTcpNoDelay(true)
                .build());
    }

    @Override
    public Response getFile(long id, int type) {
        return getFile(id, type, null);
    }

    @Override
    public Response getFile(long id, int type, String authToken) {
        try {
            BoundRequestBuilder requestBuilder = httpClient.prepareGet(baseUrl + "/" + id + "/" + type);

            if (authToken != null) {
                requestBuilder.addHeader(AUTH_TOKEN_HEADER, authToken);
            }

            return requestBuilder.execute().get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("Can't execute query", e);
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public Response addFile(@NotNull File file, @NotNull Map<String, String> headers) {
        BoundRequestBuilder requestBuilder = httpClient.preparePost(baseUrl.toString());
        return executePost(requestBuilder, file, headers);
    }

    @Override
    public Response addFile(long id, int type, @NotNull File file, @NotNull Map<String, String> headers) {
        BoundRequestBuilder requestBuilder = httpClient.preparePost(baseUrl + "/" + id + "/" + type);
        return executePost(requestBuilder, file, headers);
    }

    @Override
    public Response deleteFile(long id, @NotNull String apiToken) {
        try {
            return deleteFileAsync(id, apiToken).get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("Can't execute query", e);
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public ListenableFuture<Response> deleteFileAsync(long id, @NotNull String apiToken) {
        return httpClient.prepareDelete(baseUrl + "/" + id + "?token=" + apiToken).execute();
    }

    @Override
    public Response isMaster() {
        try {
            return httpClient.prepareGet(baseUrl + UriType.MASTER.getUri()).execute().get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("Can't execute query", e);
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public Response setMaster(@NotNull SetMasterApiRequest request) {
        BoundRequestBuilder postRequestBuilder = httpClient.preparePost(baseUrl.toString() + UriType.SET_MASTER.getUri());
        return executePost(postRequestBuilder, request);
    }

    @Override
    public Response removeMaster(@NotNull EmptyRequest request) {
        BoundRequestBuilder postRequestBuilder = httpClient.preparePost(baseUrl.toString() + UriType.REMOVE_MASTER.getUri());
        return executePost(postRequestBuilder, request);
    }

    @Override
    public Response refreshDisks(@NotNull RefreshDiskRequest request) {
        BoundRequestBuilder postRequestBuilder = httpClient.preparePost(baseUrl.toString() + UriType.REFRESH.getUri());
        return executePost(postRequestBuilder, request);
    }

    @Override
    public Response repair(@NotNull RepairDiskRequest request) {
        BoundRequestBuilder postRequestBuilder = httpClient.preparePost(baseUrl.toString() + UriType.REPAIR.getUri());
        return executePost(postRequestBuilder, request);
    }

    @Override
    protected void doStart() {
        notifyStarted();
    }

    @Override
    protected void doStop() {
        try {
            httpClient.close();
        } catch (IOException e) {
            log.error("Can't close http client", e);
        }
        notifyStopped();
    }

    private Response executePost(BoundRequestBuilder postRequestBuilder, ApiRequest request) {
        try {
            postRequestBuilder.addBodyPart(new StringPart("json", objectMapper.writeValueAsString(request)));

            return httpClient.executeRequest(postRequestBuilder.build()).get();
        } catch (InterruptedException | ExecutionException | IOException e) {
            log.error("Can't execute query", e);
            throw new IllegalArgumentException(e);
        }
    }

    private Response executePost(
            BoundRequestBuilder postRequestBuilder,
            File file,
            Map<String, String> headers
    ) {
        checkArgument(headers.containsKey("X-Metadata-Content-Type"), "X-Metadata-Content-Type header is required!");

        postRequestBuilder.addBodyPart(new FilePart("file", file));
        for (String headerName : headers.keySet()) {
            postRequestBuilder.addHeader(headerName, headers.get(headerName));
        }

        try {
            return httpClient.executeRequest(postRequestBuilder.build()).get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("Can't execute query", e);
            throw new IllegalArgumentException(e);
        }
    }
}
