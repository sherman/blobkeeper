package io.blobkeeper.server;

/*
 * Copyright (C) 2015-2016 by Denis M. Gabaydulin
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
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import io.blobkeeper.client.service.BlobKeeperClient;
import io.blobkeeper.client.service.BlobKeeperClientImpl;
import io.blobkeeper.client.util.BlobKeeperClientUtils;
import io.blobkeeper.common.configuration.RootModule;
import io.blobkeeper.common.domain.Result;
import io.blobkeeper.common.domain.api.EmptyRequest;
import io.blobkeeper.common.domain.api.MasterNode;
import io.blobkeeper.common.domain.api.SetMasterApiRequest;
import io.blobkeeper.common.util.TokenUtils;
import io.blobkeeper.file.configuration.FileConfiguration;
import io.blobkeeper.file.service.FileListService;
import io.blobkeeper.index.domain.IndexElt;
import io.blobkeeper.index.service.IndexCacheService;
import io.blobkeeper.index.service.IndexService;
import io.blobkeeper.server.configuration.ServerConfiguration;
import io.blobkeeper.server.configuration.ServerModule;
import io.blobkeeper.server.util.JsonUtils;
import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.Response;
import org.joda.time.DateTimeConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;

import static com.google.common.io.Files.write;
import static io.blobkeeper.file.util.FileUtils.getDiskPathByDisk;
import static java.io.File.createTempFile;
import static java.nio.charset.Charset.forName;
import static org.testng.Assert.*;

@Guice(modules = {RootModule.class, ServerModule.class})
public class BasicOperationTest {
    private static final Logger log = LoggerFactory.getLogger(BasicOperationTest.class);

    @Inject
    private BlobKeeperServer server;

    @Inject
    private ServerConfiguration serverConfiguration;

    @Inject
    private JsonUtils jsonUtils;

    @Inject
    private ObjectMapper objectMapper;

    @Inject
    private FileConfiguration fileConfiguration;

    @Inject
    private FileListService fileListService;

    @Inject
    private IndexService indexService;

    @Inject
    private IndexCacheService indexCacheService;

    @Inject
    private BlobKeeperClientUtils clientUtils;

    private BlobKeeperClient client;

    @Test
    public void testPipeline() throws Exception {
        File file = createTempFile(this.getClass().getName(), "");
        write("test", file, forName("UTF-8"));

        // send post query
        Response postResponse = client.addFile(file, ImmutableMap.of("X-Metadata-Content-Type", "text/plain"));

        assertEquals(postResponse.getStatusCode(), 200);
        assertTrue(postResponse.getResponseBody().contains("\"result\":{\"id\""));

        Result result = jsonUtils.getFromJson(postResponse.getResponseBody());
        assertNotNull(result.getIdLong());
        long givenId = result.getIdLong();

        // send get query
        Response getResponse = client.getFile(givenId, 0);
        assertEquals(getResponse.getStatusCode(), 200);
        assertEquals(getResponse.getResponseBody(), "test");
        assertEquals(getResponse.getContentType(), "text/plain");

        // send post query
        postResponse = client.addFile(file, ImmutableMap.of("X-Metadata-Content-Type", "text/plain"));
        assertEquals(postResponse.getStatusCode(), 200);
        assertTrue(postResponse.getResponseBody().contains("\"result\":{\"id\""));

        result = jsonUtils.getFromJson(postResponse.getResponseBody());
        assertNotNull(result.getIdLong());
        givenId = result.getIdLong();

        // send get query
        getResponse = client.getFile(givenId, 0);
        assertEquals(getResponse.getStatusCode(), 200);
        assertEquals(getResponse.getResponseBody(), "test");
        assertEquals(getResponse.getContentType(), "text/plain");

        // send delete query
        Response deleteResponse = client.deleteFile(givenId, serverConfiguration.getApiToken());
        assertEquals(deleteResponse.getStatusCode(), 200);
        assertTrue(postResponse.getResponseBody().contains("\"result\":{\"id\""));

        // send get query
        getResponse = client.getFile(givenId, 0);
        assertEquals(getResponse.getStatusCode(), 410);

        // send delete query
        deleteResponse = client.deleteFile(givenId, serverConfiguration.getApiToken());
        assertEquals(deleteResponse.getStatusCode(), 200);
        assertTrue(postResponse.getResponseBody().contains("\"result\":{\"id\""));

        // send post query
        postResponse = client.addFile(file, ImmutableMap.of("X-Metadata-Content-Type", "text/plain"));
        assertEquals(postResponse.getStatusCode(), 200);
        assertTrue(postResponse.getResponseBody().contains("\"result\":{\"id\""));

        result = jsonUtils.getFromJson(postResponse.getResponseBody());
        assertNotNull(result.getIdLong());

        // send delete query
        deleteResponse = client.deleteFile(givenId, serverConfiguration.getApiToken());
        assertEquals(deleteResponse.getStatusCode(), 200);
        assertTrue(postResponse.getResponseBody().contains("\"result\":{\"id\""));

        // send get query
        getResponse = client.getFile(givenId, 0);
        assertEquals(getResponse.getStatusCode(), 410);

        // send delete query
        deleteResponse = client.deleteFile(givenId, serverConfiguration.getApiToken());
        assertEquals(deleteResponse.getStatusCode(), 200);
        assertTrue(postResponse.getResponseBody().contains("\"result\":{\"id\""));

        // send post query
        postResponse = client.addFile(file, ImmutableMap.of("X-Metadata-Content-Type", "text/plain"));
        assertEquals(postResponse.getStatusCode(), 200);
        assertTrue(postResponse.getResponseBody().contains("\"result\":{\"id\""));

        result = jsonUtils.getFromJson(postResponse.getResponseBody());
        assertNotNull(result.getIdLong());

        // send get query
        getResponse = client.getFile(result.getIdLong(), 0);
        assertEquals(getResponse.getStatusCode(), 200);
        assertEquals(getResponse.getResponseBody(), "test");
        assertEquals(getResponse.getContentType(), "text/plain");
    }

    @Test
    public void invalidId() throws Exception {
        Response response = client.getFile(-1L, 0);
        assertEquals(response.getStatusCode(), 400);
    }

    @Test
    public void getNonexistentFile() throws Exception {
        Response response = client.getFile(424242424242424242L, 6);
        assertEquals(response.getStatusCode(), 404);
    }

    @Test
    public void getFile() throws Exception {
        File file = createTempFile(this.getClass().getName(), "");
        write("test", file, forName("UTF-8"));

        Response postResponse = client.addFile(file, ImmutableMap.of("X-Metadata-Content-Type", "text/plain"));

        assertEquals(postResponse.getStatusCode(), 200);
        assertTrue(postResponse.getResponseBody().contains("\"result\":{\"id\""));

        Result result = jsonUtils.getFromJson(postResponse.getResponseBody());
        assertNotNull(result.getIdLong());
        long givenId = result.getIdLong();

        Response getResponse = client.getFile(givenId, 0);
        assertEquals(getResponse.getStatusCode(), 200);
        assertEquals(getResponse.getResponseBody(), "test");
        assertEquals(getResponse.getContentType(), "text/plain");
    }

    @Test
    public void getAuthenticatedFileFile() throws Exception {
        File file = createTempFile(this.getClass().getName(), "");
        write("test", file, forName("UTF-8"));

        String token1 = TokenUtils.getToken(serverConfiguration.getSecretToken(), 12131345594954594L);
        String token2 = TokenUtils.getToken(serverConfiguration.getSecretToken(), 21931345521295594L);

        Response postResponse = client.addFile(file, ImmutableMap.of(
                "X-Metadata-Content-Type", "text/plain",
                "X-Metadata-Auth-Token", Joiner.on(",").join(token1, token2)
        ));

        assertEquals(postResponse.getStatusCode(), 200);
        assertTrue(postResponse.getResponseBody().contains("\"result\":{\"id\""));

        Result result = jsonUtils.getFromJson(postResponse.getResponseBody());
        assertNotNull(result.getIdLong());
        long givenId = result.getIdLong();

        Response getResponse = client.getFile(givenId, 0);
        assertEquals(getResponse.getStatusCode(), 403);

        getResponse = client.getFile(givenId, 0, token1);
        assertEquals(getResponse.getStatusCode(), 200);
        assertEquals(getResponse.getResponseBody(), "test");
        assertEquals(getResponse.getContentType(), "text/plain");

        getResponse = client.getFile(givenId, 0, token2);
        assertEquals(getResponse.getStatusCode(), 200);
        assertEquals(getResponse.getResponseBody(), "test");
        assertEquals(getResponse.getContentType(), "text/plain");

        getResponse = client.getFile(givenId, 0, TokenUtils.getToken(serverConfiguration.getSecretToken(), 42L));
        assertEquals(getResponse.getStatusCode(), 403);
    }

    @Test
    public void emptyAuthToken() throws IOException {
        File file = createTempFile(this.getClass().getName(), "");
        write("test", file, forName("UTF-8"));

        Response postResponse = client.addFile(file, ImmutableMap.of(
                "X-Metadata-Content-Type", "text/plain",
                "X-Metadata-Auth-Token", ""
        ));

        assertEquals(postResponse.getStatusCode(), 200);
        assertTrue(postResponse.getResponseBody().contains("\"result\":{\"id\""));

        Result result = jsonUtils.getFromJson(postResponse.getResponseBody());
        assertNotNull(result.getIdLong());
        long givenId = result.getIdLong();

        Response getResponse = client.getFile(givenId, 0);
        assertEquals(getResponse.getStatusCode(), 403);

        getResponse = client.getFile(givenId, 0, TokenUtils.getToken(serverConfiguration.getSecretToken(), 42L));
        assertEquals(getResponse.getStatusCode(), 403);
    }

    @Test
    public void getNotModifiedFile() throws Exception {
        File file = createTempFile(this.getClass().getName(), "");
        write("test", file, forName("UTF-8"));

        Response postResponse = client.addFile(file, ImmutableMap.of("X-Metadata-Content-Type", "text/plain"));

        assertEquals(postResponse.getStatusCode(), 200);
        assertTrue(postResponse.getResponseBody().contains("\"result\":{\"id\""));

        Result result = jsonUtils.getFromJson(postResponse.getResponseBody());
        assertNotNull(result.getIdLong());
        long givenId = result.getIdLong();

        Response getResponse = client.getFile(givenId, 0);
        assertEquals(getResponse.getStatusCode(), 200);
        assertEquals(getResponse.getResponseBody(), "test");
        assertEquals(getResponse.getContentType(), "text/plain");

        String value = getResponse.getHeader("last-modified");
        assertNotNull(value);

        AsyncHttpClient httpClient = new DefaultAsyncHttpClient();

        getResponse = httpClient.prepareGet(serverConfiguration.getBaseUrl().toString() + "/" + givenId + "/0")
                .addHeader("if-modified-since", value)
                .execute()
                .get();

        assertEquals(getResponse.getStatusCode(), 304);
        assertEquals(getResponse.getResponseBody(), "");
        assertEquals(getResponse.getContentType(), "text/plain");

        // ignore case sensitive
        getResponse = httpClient.prepareGet(serverConfiguration.getBaseUrl().toString() + "/" + givenId + "/0")
                .addHeader("If-Modified-Since", value)
                .execute()
                .get();

        assertEquals(getResponse.getStatusCode(), 304);
        assertEquals(getResponse.getResponseBody(), "");
        assertEquals(getResponse.getContentType(), "text/plain");
    }

    @Test
    public void modifyFile() throws Exception {
        File file = createTempFile(this.getClass().getName(), "");
        write("test", file, forName("UTF-8"));

        Response postResponse = client.addFile(file, ImmutableMap.of("X-Metadata-Content-Type", "text/plain"));

        assertEquals(postResponse.getStatusCode(), 200);
        assertTrue(postResponse.getResponseBody().contains("\"result\":{\"id\""));

        Result result = jsonUtils.getFromJson(postResponse.getResponseBody());
        assertNotNull(result.getIdLong());
        long givenId = result.getIdLong();

        Response getResponse = client.getFile(givenId, 0);
        assertEquals(getResponse.getStatusCode(), 200);
        assertEquals(getResponse.getResponseBody(), "test");
        assertEquals(getResponse.getContentType(), "text/plain");

        String value = getResponse.getHeader("last-modified");
        assertNotNull(value);

        AsyncHttpClient httpClient = new DefaultAsyncHttpClient();

        getResponse = httpClient.prepareGet(serverConfiguration.getBaseUrl().toString() + "/" + givenId + "/0")
                .addHeader("if-modified-since", value)
                .execute()
                .get();

        assertEquals(getResponse.getStatusCode(), 304);
        assertEquals(getResponse.getResponseBody(), "");
        assertEquals(getResponse.getContentType(), "text/plain");

        // modify file
        IndexElt elt = indexService.getById(givenId, 0);
        IndexElt modified = new IndexElt.IndexEltBuilder()
                .id(elt.getId())
                .type(elt.getType())
                .partition(elt.getPartition())
                .length(elt.getLength())
                .offset(elt.getOffset())
                .crc(elt.getCrc())
                .metadata(elt.getMetadata())
                .deleted(elt.isDeleted())
                .created(elt.getCreated() + DateTimeConstants.MILLIS_PER_SECOND + 1)
                .build();

        indexService.add(modified);
        indexCacheService.remove(modified.toCacheKey());

        // ignore case sensitive
        getResponse = httpClient.prepareGet(serverConfiguration.getBaseUrl().toString() + "/" + givenId + "/0")
                .addHeader("If-Modified-Since", value)
                .execute()
                .get();

        assertEquals(getResponse.getStatusCode(), 200);
        assertEquals(getResponse.getResponseBody(), "test");
        assertEquals(getResponse.getContentType(), "text/plain");
    }

    @Test
    public void getThumb() throws Exception {
        File file = createTempFile(this.getClass().getName(), "");
        write("test", file, forName("UTF-8"));

        Response postResponse = client.addFile(file, ImmutableMap.of("X-Metadata-Content-Type", "text/plain"));

        assertEquals(postResponse.getStatusCode(), 200);
        assertTrue(postResponse.getResponseBody().contains("\"result\":{\"id\""));

        Result result = jsonUtils.getFromJson(postResponse.getResponseBody());
        assertNotNull(result.getIdLong());
        long givenId = result.getIdLong();

        Response getResponse = client.getFile(givenId, 0);
        assertEquals(getResponse.getStatusCode(), 200);
        assertEquals(getResponse.getResponseBody(), "test");
        assertEquals(getResponse.getContentType(), "text/plain");

        getResponse = client.getFile(givenId, 1);
        assertEquals(getResponse.getStatusCode(), 404);

        // upload another type for given id
        postResponse = client.addFile(givenId, 1, file, ImmutableMap.of("X-Metadata-Content-Type", "text/plain"));
        assertEquals(postResponse.getStatusCode(), 200);
        assertTrue(postResponse.getResponseBody().contains("\"result\":{\"id\""));

        getResponse = client.getFile(givenId, 1);
        assertEquals(getResponse.getStatusCode(), 200);
        assertEquals(getResponse.getResponseBody(), "test");
        assertEquals(getResponse.getContentType(), "text/plain");
    }

    @Test
    public void invalidThumb() throws Exception {
        File file = createTempFile(this.getClass().getName(), "");
        write("test", file, forName("UTF-8"));

        Response postResponse = client.addFile(file, ImmutableMap.of("X-Metadata-Content-Type", "text/plain"));

        assertEquals(postResponse.getStatusCode(), 200);
        assertTrue(postResponse.getResponseBody().contains("\"result\":{\"id\""));

        Result result = jsonUtils.getFromJson(postResponse.getResponseBody());
        assertNotNull(result.getIdLong());
        long givenId = result.getIdLong();

        Response getResponse = client.getFile(givenId, 0);
        assertEquals(getResponse.getStatusCode(), 200);
        assertEquals(getResponse.getResponseBody(), "test");
        assertEquals(getResponse.getContentType(), "text/plain");

        getResponse = client.getFile(givenId, 1);
        assertEquals(getResponse.getStatusCode(), 404);

        // upload another type for given id
        postResponse = client.addFile(givenId, 1, file, ImmutableMap.of("X-Metadata-Content-Type", "text/plain"));
        assertEquals(postResponse.getStatusCode(), 200);
        assertTrue(postResponse.getResponseBody().contains("\"result\":{\"id\""));

        getResponse = client.getFile(givenId, 1);
        assertEquals(getResponse.getStatusCode(), 200);
        assertEquals(getResponse.getResponseBody(), "test");
        assertEquals(getResponse.getContentType(), "text/plain");

        postResponse = client.addFile(givenId, 1, file, ImmutableMap.of("X-Metadata-Content-Type", "text/plain"));
        assertEquals(postResponse.getStatusCode(), 409);
    }

    @Test
    public void invalidUploadRequest() throws Exception {
        AsyncHttpClient httpClient = new DefaultAsyncHttpClient();

        BoundRequestBuilder boundRequestBuilder = httpClient.preparePost(serverConfiguration.getBaseUrl().toString());
        boundRequestBuilder
                .addHeader("X-Metadata-Content-Type", "text/plain");
        Response postResponse = httpClient.executeRequest(boundRequestBuilder.build()).get();

        assertEquals(postResponse.getStatusCode(), 400);
        assertEquals(postResponse.getResponseBody(), "{\"error\":{\"code\":\"INVALID_REQUEST\",\"message\":\"There is no upload file in the request\"}}");

        httpClient.close();
    }

    @Test
    public void deleteFile() throws Exception {
        File file = createTempFile(this.getClass().getName(), "");
        write("testtest", file, forName("UTF-8"));

        Response postResponse = client.addFile(file, ImmutableMap.of("X-Metadata-Content-Type", "text/plain"));

        assertEquals(postResponse.getStatusCode(), 200);
        assertTrue(postResponse.getResponseBody().contains("\"result\":{\"id\""));

        Result result = jsonUtils.getFromJson(postResponse.getResponseBody());
        assertNotNull(result.getIdLong());
        long givenId = result.getIdLong();

        // check saved file
        Response getResponse = client.getFile(givenId, 0);
        assertEquals(getResponse.getStatusCode(), 200);
        assertEquals(getResponse.getResponseBody(), "testtest");
        assertEquals(getResponse.getContentType(), "text/plain");

        // delete it
        Response deleteResponse = client.deleteFile(givenId, serverConfiguration.getApiToken());
        assertEquals(deleteResponse.getStatusCode(), 200);
        assertTrue(postResponse.getResponseBody().contains("\"result\":{\"id\""));

        // check it again
        getResponse = client.getFile(givenId, 0);
        assertEquals(getResponse.getStatusCode(), 410);
    }

    @Test
    public void invalidDeleteFile() throws Exception {
        File file = createTempFile(this.getClass().getName(), "");
        write("testtest", file, forName("UTF-8"));

        Response postResponse = client.addFile(file, ImmutableMap.of("X-Metadata-Content-Type", "text/plain"));

        assertEquals(postResponse.getStatusCode(), 200);
        assertTrue(postResponse.getResponseBody().contains("\"result\":{\"id\""));

        Result result = jsonUtils.getFromJson(postResponse.getResponseBody());
        assertNotNull(result.getIdLong());
        long givenId = result.getIdLong();

        // check saved file
        Response getResponse = client.getFile(givenId, 0);
        assertEquals(getResponse.getStatusCode(), 200);
        assertEquals(getResponse.getResponseBody(), "testtest");
        assertEquals(getResponse.getContentType(), "text/plain");

        // invalid token
        Response deleteResponse = client.deleteFile(givenId, "");
        assertEquals(deleteResponse.getStatusCode(), 400);

        // invalid file
        deleteResponse = client.deleteFile(-1L, serverConfiguration.getApiToken());
        assertEquals(deleteResponse.getStatusCode(), 400);

        // nonexistent file
        deleteResponse = client.deleteFile(givenId + 1, serverConfiguration.getApiToken());
        assertEquals(deleteResponse.getStatusCode(), 400);
    }

    @Test
    public void setInvalidMaster() throws IOException {
        AsyncHttpClient httpClient = new DefaultAsyncHttpClient();

        MasterNode masterNode = new MasterNode();
        masterNode.setAddress("invalid");

        SetMasterApiRequest request = new SetMasterApiRequest();
        request.setNode(masterNode);
        request.setToken(serverConfiguration.getApiToken());

        Response response = client.setMaster(request);
        assertEquals(response.getStatusCode(), 400);
        assertEquals(response.getResponseBody(), "{\"error\":{\"code\":\"NODE_IS_NOT_EXISTS\",\"message\":\"Node is not exists!\"}}");

        httpClient.close();
    }

    @Test
    public void setMaster() throws IOException {
        AsyncHttpClient httpClient = new DefaultAsyncHttpClient();

        MasterNode masterNode = new MasterNode();
        masterNode.setAddress(serverConfiguration.getServerName());

        SetMasterApiRequest request = new SetMasterApiRequest();
        request.setNode(masterNode);
        request.setToken(serverConfiguration.getApiToken());

        Response response = client.setMaster(request);
        assertEquals(response.getStatusCode(), 200);
        assertEquals(response.getResponseBody(), "{\"result\":true}");

        httpClient.close();
    }

    @Test
    public void masterIsRequired() throws Exception {
        AsyncHttpClient httpClient = new DefaultAsyncHttpClient();

        assertEquals(client.isMaster().getResponseBody(), "{\"result\":true}");

        EmptyRequest request = new EmptyRequest();
        request.setToken(serverConfiguration.getApiToken());

        Response postResponse = client.removeMaster(request);
        assertEquals(postResponse.getStatusCode(), 200);
        assertEquals(postResponse.getResponseBody(), "{\"result\":true}");

        assertEquals(client.isMaster().getResponseBody(), "{\"result\":false}");

        File file = createTempFile(this.getClass().getName(), "");
        write("test", file, forName("UTF-8"));

        // send post query
        postResponse = client.addFile(file, ImmutableMap.of("X-Metadata-Content-Type", "text/plain"));

        assertEquals(postResponse.getStatusCode(), 405);
        assertEquals(postResponse.getResponseBody(), "{\"error\":{\"code\":\"NOT_A_MASTER\",\"message\":\"Node is not a master\"}}");

        MasterNode masterNode = new MasterNode();
        masterNode.setAddress(serverConfiguration.getServerName());

        SetMasterApiRequest masterApiRequest = new SetMasterApiRequest();
        masterApiRequest.setNode(masterNode);
        masterApiRequest.setToken(serverConfiguration.getApiToken());

        client.setMaster(masterApiRequest);

        httpClient.close();
    }

    @BeforeClass
    private void start() throws Exception {
        deleteIndexFiles();
        clearIndex();

        server.startAsync();
        server.awaitRunning();

        client = new BlobKeeperClientImpl(objectMapper, serverConfiguration.getBaseUrl());
        client.startAsync();
        client.awaitRunning();

        clientUtils.waitForMaster(client);
    }

    @AfterClass
    private void stop() throws InterruptedException {
        server.stopAsync();
        server.awaitTerminated();

        client.stopAsync();
    }

    private void clearIndex() {
        indexService.clear();
    }

    private void deleteIndexFiles() {
        java.io.File basePath = new java.io.File(fileConfiguration.getBasePath());
        if (!basePath.exists()) {
            basePath.mkdir();
        }

        java.io.File disk1 = getDiskPathByDisk(fileConfiguration, 0);
        if (!disk1.exists()) {
            disk1.mkdir();
        }

        java.io.File disk2 = getDiskPathByDisk(fileConfiguration, 1);
        if (!disk2.exists()) {
            disk2.mkdir();
        }

        for (int disk : fileListService.getDisks()) {
            java.io.File diskPath = getDiskPathByDisk(fileConfiguration, disk);
            for (java.io.File indexFile : diskPath.listFiles((FileFilter) new SuffixFileFilter(".data"))) {
                indexFile.delete();
            }
        }

        java.io.File uploadPath = new java.io.File(fileConfiguration.getUploadPath());
        if (!uploadPath.exists()) {
            uploadPath.mkdir();
        }
    }
}
