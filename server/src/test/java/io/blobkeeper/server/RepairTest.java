package io.blobkeeper.server;

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
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import io.blobkeeper.client.service.BlobKeeperClient;
import io.blobkeeper.client.service.BlobKeeperClientImpl;
import io.blobkeeper.cluster.service.ClusterMembershipService;
import io.blobkeeper.common.domain.Result;
import io.blobkeeper.file.configuration.FileConfiguration;
import io.blobkeeper.file.service.BaseMultipleInjectorFileTest;
import io.blobkeeper.server.configuration.ServerConfiguration;
import io.blobkeeper.server.service.FileWriterService;
import io.blobkeeper.server.util.JsonUtils;
import org.asynchttpclient.Response;
import org.slf4j.Logger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Arrays;

import static com.google.common.io.Files.write;
import static java.io.File.createTempFile;
import static java.lang.Thread.sleep;
import static java.nio.charset.Charset.forName;
import static org.slf4j.LoggerFactory.getLogger;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class RepairTest extends BaseMultipleInjectorFileTest {
    private static final Logger log = getLogger(RepairTest.class);

    private BlobKeeperClient client1;
    private BlobKeeperClient client2;

    private BlobKeeperServer server1;
    private BlobKeeperServer server2;
    private BlobKeeperServer restartedServer2;

    @Test
    public void replicateFile() throws Exception {
        startServer1(10000);
        startServer2(10000);

        File file = createTempFile(this.getClass().getName(), "");
        write("test", file, forName("UTF-8"));

        Response postResponse = client1.addFile(file, ImmutableMap.of("X-Metadata-Content-Type", "text/plain"));
        assertEquals(postResponse.getStatusCode(), 200);
        assertTrue(postResponse.getResponseBody().contains("\"result\":{\"id\""));

        Result result = firstServerInjector.getInstance(JsonUtils.class).getFromJson(postResponse.getResponseBody());

        Thread.sleep(30);

        Response getResponse = client1.getFile(result.getIdLong(), 0);
        assertEquals(getResponse.getStatusCode(), 200);
        assertEquals(getResponse.getResponseBody(), "test");
        assertEquals(getResponse.getContentType(), "text/plain");

        getResponse = client2.getFile(result.getIdLong(), 0);
        assertEquals(getResponse.getStatusCode(), 200);
        assertEquals(getResponse.getResponseBody(), "test");
        assertEquals(getResponse.getContentType(), "text/plain");
    }

    @Test
    public void addNewSlave() throws Exception {
        startServer1(10000);

        File file = createTempFile(this.getClass().getName(), "");
        write(Strings.repeat("test42", 10240), file, forName("UTF-8"));

        // add a file
        Response postResponse = client1.addFile(file, ImmutableMap.of("X-Metadata-Content-Type", "text/plain"));

        assertEquals(postResponse.getStatusCode(), 200);
        assertTrue(postResponse.getResponseBody().contains("\"result\":{\"id\""));

        Result result = firstServerInjector.getInstance(JsonUtils.class).getFromJson(postResponse.getResponseBody());

        sleep(100);

        Response getResponse = client1.getFile(result.getIdLong(), 0);
        assertEquals(getResponse.getStatusCode(), 200);
        assertEquals(getResponse.getResponseBody().length(), "test42".length() * 10240);
        assertEquals(getResponse.getContentType(), "text/plain");

        // add another file
        postResponse = client1.addFile(file, ImmutableMap.of("X-Metadata-Content-Type", "text/plain"));

        assertEquals(postResponse.getStatusCode(), 200);
        assertTrue(postResponse.getResponseBody().contains("\"result\":{\"id\""));

        result = firstServerInjector.getInstance(JsonUtils.class).getFromJson(postResponse.getResponseBody());

        sleep(100);

        getResponse = client1.getFile(result.getIdLong(), 0);
        assertEquals(getResponse.getStatusCode(), 200);
        assertEquals(getResponse.getResponseBody().length(), "test42".length() * 10240);
        assertEquals(getResponse.getContentType(), "text/plain");

        sleep(100);

        // add slave node
        startServer2(10000);

        sleep(2000);

        getResponse = client2.getFile(result.getIdLong(), 0);
        assertEquals(getResponse.getStatusCode(), 200);
        assertEquals(getResponse.getResponseBody().length(), "test42".length() * 10240);

        byte[] firstBytes = Arrays.copyOfRange(getResponse.getResponseBody().getBytes(), 0, 10);
        assertEquals(new String(firstBytes), "test42test");
        assertEquals(getResponse.getContentType(), "text/plain");
    }

    @Test
    public void syncSlave() throws Exception {
        addNewSlave();

        stopServer2();

        File file = createTempFile(this.getClass().getName(), "");
        write(Strings.repeat("test42", 10240), file, forName("UTF-8"));

        // add one more file
        Response postResponse = client1.addFile(file, ImmutableMap.of("X-Metadata-Content-Type", "text/plain"));

        assertEquals(postResponse.getStatusCode(), 200);
        assertTrue(postResponse.getResponseBody().contains("\"result\":{\"id\""));

        Result result = firstServerInjector.getInstance(JsonUtils.class).getFromJson(postResponse.getResponseBody());

        sleep(100);

        Response getResponse = client1.getFile(result.getIdLong(), 0);
        assertEquals(getResponse.getStatusCode(), 200);
        assertEquals(getResponse.getResponseBody().length(), "test42".length() * 10240);
        assertEquals(getResponse.getContentType(), "text/plain");

        // start slave node again
        restartServer2(10000);

        sleep(2000);

        getResponse = client2.getFile(result.getIdLong(), 0);
        assertEquals(getResponse.getStatusCode(), 200);
        assertEquals(getResponse.getResponseBody().length(), "test42".length() * 10240);

        byte[] firstBytes = Arrays.copyOfRange(getResponse.getResponseBody().getBytes(), 0, 10);
        assertEquals(new String(firstBytes), "test42test");
        assertEquals(getResponse.getContentType(), "text/plain");
    }

    @Test
    public void replicateOnNonexistentDisk() throws Exception {
        removeDisk(firstServerInjector, 1);
        removeDisk(secondServerInjector, 0);

        // master
        startServer1(10000);
        // slave
        startServer2(10000);

        File file = createTempFile(this.getClass().getName(), "");
        write(Strings.repeat("test42", 10240), file, forName("UTF-8"));

        // add a file
        Response postResponse = client1.addFile(file, ImmutableMap.of("X-Metadata-Content-Type", "text/plain"));

        assertEquals(postResponse.getStatusCode(), 200);
        assertTrue(postResponse.getResponseBody().contains("\"result\":{\"id\""));

        Result result = firstServerInjector.getInstance(JsonUtils.class).getFromJson(postResponse.getResponseBody());

        sleep(100);

        Response getResponse = client1.getFile(result.getIdLong(), 0);
        assertEquals(getResponse.getStatusCode(), 200);
        assertEquals(getResponse.getResponseBody().length(), "test42".length() * 10240);
        assertEquals(getResponse.getContentType(), "text/plain");

        // add another file
        postResponse = client1.addFile(file, ImmutableMap.of("X-Metadata-Content-Type", "text/plain"));

        assertEquals(postResponse.getStatusCode(), 200);
        assertTrue(postResponse.getResponseBody().contains("\"result\":{\"id\""));

        result = firstServerInjector.getInstance(JsonUtils.class).getFromJson(postResponse.getResponseBody());

        sleep(500);

        getResponse = client1.getFile(result.getIdLong(), 0);
        assertEquals(getResponse.getStatusCode(), 200);
        assertEquals(getResponse.getResponseBody().length(), "test42".length() * 10240);
        assertEquals(getResponse.getContentType(), "text/plain");

        sleep(100);

        getResponse = client2.getFile(result.getIdLong(), 0);
        assertEquals(getResponse.getStatusCode(), 502);
    }

    @Test
    public void repairOnNewDiskEvent() throws Exception {
        removeDisk(firstServerInjector, 1);
        removeDisk(secondServerInjector, 0);

        // master
        startServer1(10000);
        // slave
        startServer2(10000);

        File file = createTempFile(this.getClass().getName(), "");
        write(Strings.repeat("test42", 10240), file, forName("UTF-8"));

        // add a file
        Response postResponse = client1.addFile(file, ImmutableMap.of("X-Metadata-Content-Type", "text/plain"));

        assertEquals(postResponse.getStatusCode(), 200);
        assertTrue(postResponse.getResponseBody().contains("\"result\":{\"id\""));

        Result result = firstServerInjector.getInstance(JsonUtils.class).getFromJson(postResponse.getResponseBody());

        sleep(100);

        Response getResponse = client1.getFile(result.getIdLong(), 0);
        assertEquals(getResponse.getStatusCode(), 200);
        assertEquals(getResponse.getResponseBody().length(), "test42".length() * 10240);
        assertEquals(getResponse.getContentType(), "text/plain");

        // add another file
        postResponse = client1.addFile(file, ImmutableMap.of("X-Metadata-Content-Type", "text/plain"));

        assertEquals(postResponse.getStatusCode(), 200);
        assertTrue(postResponse.getResponseBody().contains("\"result\":{\"id\""));

        result = firstServerInjector.getInstance(JsonUtils.class).getFromJson(postResponse.getResponseBody());

        sleep(500);

        getResponse = client1.getFile(result.getIdLong(), 0);
        assertEquals(getResponse.getStatusCode(), 200);
        assertEquals(getResponse.getResponseBody().length(), "test42".length() * 10240);
        assertEquals(getResponse.getContentType(), "text/plain");

        sleep(500);

        getResponse = client2.getFile(result.getIdLong(), 0);
        assertEquals(getResponse.getStatusCode(), 502);

        // ok let's add disk and reconfigure server 2
        addDisk(secondServerInjector, 0);
        secondServerInjector.getInstance(FileWriterService.class).refresh();

        sleep(500);

        getResponse = client2.getFile(result.getIdLong(), 0);
        assertEquals(getResponse.getStatusCode(), 200);
        assertEquals(getResponse.getResponseBody().length(), "test42".length() * 10240);
        assertEquals(getResponse.getContentType(), "text/plain");
    }

    @Test
    public void syncCachesWhenDeleteFile() throws Exception {
        startServer1(10000);
        startServer2(10000);

        File file = createTempFile(this.getClass().getName(), "");
        write("test", file, forName("UTF-8"));

        Response postResponse = client1.addFile(file, ImmutableMap.of("X-Metadata-Content-Type", "text/plain"));
        assertEquals(postResponse.getStatusCode(), 200);
        assertTrue(postResponse.getResponseBody().contains("\"result\":{\"id\""));

        Result result = firstServerInjector.getInstance(JsonUtils.class).getFromJson(postResponse.getResponseBody());

        Thread.sleep(30);

        Response getResponse = client1.getFile(result.getIdLong(), 0);
        assertEquals(getResponse.getStatusCode(), 200);
        assertEquals(getResponse.getResponseBody(), "test");
        assertEquals(getResponse.getContentType(), "text/plain");

        getResponse = client2.getFile(result.getIdLong(), 0);
        assertEquals(getResponse.getStatusCode(), 200);
        assertEquals(getResponse.getResponseBody(), "test");
        assertEquals(getResponse.getContentType(), "text/plain");

        Response deleteResponse = client1.deleteFile(result.getIdLong(), firstServerInjector.getInstance(ServerConfiguration.class).getApiToken());
        assertEquals(deleteResponse.getStatusCode(), 200);

        getResponse = client1.getFile(result.getIdLong(), 0);
        assertEquals(getResponse.getStatusCode(), 410);

        getResponse = client2.getFile(result.getIdLong(), 0);
        assertEquals(getResponse.getStatusCode(), 410);
    }

    //@Test
    public void bigFileRepair() throws Exception {
        startServer1(1024 * 1024 * 512);

        File file = createTempFile(this.getClass().getName(), "");
        byte[] data = new byte[1024 * 1024];
        for (int i = 0; i < data.length; i++) {
            data[i] = 0x1;
        }
        write(data, file);

        for (int i = 0; i < 512; i++) {
            Response postResponse = client1.addFile(file, ImmutableMap.of("X-Metadata-Content-Type", "text/plain"));

            assertEquals(postResponse.getStatusCode(), 200);
            assertTrue(postResponse.getResponseBody().contains("\"result\":{\"id\""));
        }

        sleep(1000);

        // add salve node
        startServer2(1024 * 1024 * 512);

        // wait for all messages on the slave
        ClusterMembershipService clusterJoinedService = secondServerInjector.getInstance(ClusterMembershipService.class);
        while (clusterJoinedService.getChannel().getReceivedMessages() < 516) {
            sleep(100);
        }
    }

    @BeforeMethod(dependsOnMethods = "createInjectors")
    private void startServer() throws Exception {
        prepareDataDirectory(firstServerInjector);
        prepareDataDirectory(secondServerInjector);

        clearIndex();

        client1 = new BlobKeeperClientImpl(
                firstServerInjector.getInstance(ObjectMapper.class),
                firstServerInjector.getInstance(ServerConfiguration.class).getBaseUrl()
        );
        client1.startAsync();
        client1.awaitRunning();

        client2 = new BlobKeeperClientImpl(
                secondServerInjector.getInstance(ObjectMapper.class),
                secondServerInjector.getInstance(ServerConfiguration.class).getBaseUrl()
        );
        client2.startAsync();
        client2.awaitRunning();
    }

    private void startServer1(long fileMaxSize) {
        FileConfiguration fileConfiguration = firstServerInjector.getInstance(FileConfiguration.class);
        fileConfiguration.setMaxFileSize(fileMaxSize);
        server1 = firstServerInjector.getInstance(BlobKeeperServer.class);
        server1.startAsync();
        server1.awaitRunning();
    }

    private void startServer2(long fileMaxSize) {
        FileConfiguration fileConfiguration = secondServerInjector.getInstance(FileConfiguration.class);
        fileConfiguration.setMaxFileSize(fileMaxSize);
        server2 = secondServerInjector.getInstance(BlobKeeperServer.class);
        server2.startAsync();
        server2.awaitRunning();
    }

    private void restartServer2(long fileMaxSize) {
        FileConfiguration fileConfiguration = secondRestartedServerInjector.getInstance(FileConfiguration.class);
        fileConfiguration.setMaxFileSize(fileMaxSize);
        restartedServer2 = secondRestartedServerInjector.getInstance(BlobKeeperServer.class);
        restartedServer2.startAsync();
        restartedServer2.awaitRunning();
    }

    private void stopServer2() {
        server2 = secondServerInjector.getInstance(BlobKeeperServer.class);
        server2.stopAsync();
        server2.awaitTerminated();
    }

    @AfterMethod
    private void stopServer() {
        if (null != server2) {
            server2.stopAsync();
            server2.awaitTerminated();
        }

        if (null != server1) {
            server1.stopAsync();
            server1.awaitTerminated();
        }

        if (null != restartedServer2) {
            restartedServer2.stopAsync();
            restartedServer2.awaitTerminated();
        }

        client1.stopAsync();
        client2.stopAsync();
    }
}
