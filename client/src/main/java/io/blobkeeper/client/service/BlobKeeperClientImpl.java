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
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.ListenableFuture;
import com.ning.http.client.Response;
import com.ning.http.client.StringPart;
import com.ning.http.multipart.FilePart;
import io.blobkeeper.common.domain.api.ApiRequest;
import io.blobkeeper.common.domain.api.EmptyRequest;
import io.blobkeeper.common.domain.api.SetMasterApiRequest;
import io.blobkeeper.common.domain.api.UriType;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
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

    public BlobKeeperClientImpl(ObjectMapper objectMapper, URL baseUrl) {
        this.objectMapper = objectMapper;
        this.baseUrl = baseUrl;
        httpClient = new AsyncHttpClient();
    }

    @Override
    public Response getFile(long id, int type) {
        return getFile(id, type, null);
    }

    @Override
    public Response getFile(long id, int type, String authToken) {
        try {
            AsyncHttpClient.BoundRequestBuilder requestBuilder = httpClient.prepareGet(baseUrl + "/" + id + "/" + type);

            if (authToken != null) {
                requestBuilder.addHeader(AUTH_TOKEN_HEADER, authToken);
            }

            return requestBuilder.execute().get();
        } catch (InterruptedException | ExecutionException | IOException e) {
            log.error("Can't execute query", e);
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public Response addFile(@NotNull File file, @NotNull Map<String, String> headers) {
        AsyncHttpClient.BoundRequestBuilder requestBuilder = httpClient.preparePost(baseUrl.toString());
        return executePost(requestBuilder, file, headers);
    }

    @Override
    public Response addFile(long id, int type, @NotNull File file, @NotNull Map<String, String> headers) {
        AsyncHttpClient.BoundRequestBuilder requestBuilder = httpClient.preparePost(baseUrl + "/" + id + "/" + type);
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
        try {
            return httpClient.prepareDelete(baseUrl + "/" + id + "?token=" + apiToken).execute();
        } catch (IOException e) {
            log.error("Can't execute query", e);
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public Response isMaster() {
        try {
            return httpClient.prepareGet(baseUrl + UriType.MASTER.getUri()).execute().get();
        } catch (InterruptedException | ExecutionException | IOException e) {
            log.error("Can't execute query", e);
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public Response setMaster(@NotNull SetMasterApiRequest request) {
        AsyncHttpClient.BoundRequestBuilder postRequestBuilder = httpClient.preparePost(baseUrl.toString() + UriType.SET_MASTER.getUri());
        return executePost(postRequestBuilder, request);
    }

    @Override
    public Response removeMaster(@NotNull EmptyRequest request) {
        AsyncHttpClient.BoundRequestBuilder postRequestBuilder = httpClient.preparePost(baseUrl.toString() + UriType.REMOVE_MASTER.getUri());
        return executePost(postRequestBuilder, request);
    }

    @Override
    protected void doStart() {
        notifyStarted();
    }

    @Override
    protected void doStop() {
        httpClient.close();
        notifyStopped();
    }

    private Response executePost(AsyncHttpClient.BoundRequestBuilder postRequestBuilder, ApiRequest request) {
        try {
            postRequestBuilder.addBodyPart(new StringPart("json", objectMapper.writeValueAsString(request)));

            return httpClient.executeRequest(postRequestBuilder.build()).get();
        } catch (InterruptedException | ExecutionException | IOException e) {
            log.error("Can't execute query", e);
            throw new IllegalArgumentException(e);
        }
    }

    private Response executePost(
            AsyncHttpClient.BoundRequestBuilder postRequestBuilder,
            File file,
            Map<String, String> headers
    ) {
        checkArgument(headers.containsKey("X-Metadata-Content-Type"), "X-Metadata-Content-Type header is required!");

        try {
            postRequestBuilder.addBodyPart(new FilePart("file", file));
            for (String headerName : headers.keySet()) {
                postRequestBuilder.addHeader(headerName, headers.get(headerName));
            }
        } catch (FileNotFoundException e) {
            log.error("Can't find file", e);
            throw new IllegalArgumentException(e);
        }

        try {
            return httpClient.executeRequest(postRequestBuilder.build()).get();
        } catch (InterruptedException | ExecutionException | IOException e) {
            log.error("Can't execute query", e);
            throw new IllegalArgumentException(e);
        }
    }
}
