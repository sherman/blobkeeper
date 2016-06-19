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
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Module;
import io.blobkeeper.client.service.BlobKeeperClient;
import io.blobkeeper.client.service.BlobKeeperClientImpl;
import io.blobkeeper.cluster.service.ClusterMembershipService;
import io.blobkeeper.common.configuration.MetricModule;
import io.blobkeeper.common.domain.Result;
import io.blobkeeper.common.domain.api.RepairDiskRequest;
import io.blobkeeper.common.service.FirstServerRootModule;
import io.blobkeeper.common.service.SecondRestartedServerRootModule;
import io.blobkeeper.common.service.SecondServerRootModule;
import io.blobkeeper.common.service.ThirdServerRootModule;
import io.blobkeeper.file.configuration.FileConfiguration;
import io.blobkeeper.file.service.BaseMultipleInjectorFileTest;
import io.blobkeeper.server.configuration.ServerConfiguration;
import io.blobkeeper.server.configuration.ServerModule;
import io.blobkeeper.server.service.FileWriterService;
import io.blobkeeper.server.util.JsonUtils;
import org.asynchttpclient.Response;
import org.slf4j.Logger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.google.common.io.Files.write;
import static com.jayway.awaitility.Awaitility.await;
import static com.jayway.awaitility.Duration.ONE_HUNDRED_MILLISECONDS;
import static io.blobkeeper.server.TestUtils.assertResponseOk;
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
    private BlobKeeperClient client3;

    private BlobKeeperServer server1;
    private BlobKeeperServer server2;
    private BlobKeeperServer server3;
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
        assertResponseOk(getResponse, "test", "text/plain");

        getResponse = client2.getFile(result.getIdLong(), 0);
        assertResponseOk(getResponse, "test", "text/plain");
    }

    @Test
    public void addNewSlave() throws Exception {
        startServer1(10000);

        String expectedBody = Strings.repeat("test42", 10240);

        File file = createTempFile(this.getClass().getName(), "");
        write(expectedBody, file, forName("UTF-8"));

        // add a file
        Response postResponse = client1.addFile(file, ImmutableMap.of("X-Metadata-Content-Type", "text/plain"));

        assertEquals(postResponse.getStatusCode(), 200);
        assertTrue(postResponse.getResponseBody().contains("\"result\":{\"id\""));

        Result result = firstServerInjector.getInstance(JsonUtils.class).getFromJson(postResponse.getResponseBody());

        sleep(100);

        Response getResponse = client1.getFile(result.getIdLong(), 0);
        assertResponseOk(getResponse, expectedBody, "text/plain");

        // add another file
        postResponse = client1.addFile(file, ImmutableMap.of("X-Metadata-Content-Type", "text/plain"));

        assertEquals(postResponse.getStatusCode(), 200);
        assertTrue(postResponse.getResponseBody().contains("\"result\":{\"id\""));

        result = firstServerInjector.getInstance(JsonUtils.class).getFromJson(postResponse.getResponseBody());

        sleep(100);

        getResponse = client1.getFile(result.getIdLong(), 0);
        assertResponseOk(getResponse, expectedBody, "text/plain");

        sleep(100);

        // add small file to active partition
        String expectedSmallBody = Strings.repeat("test42", 8);
        File smallFile = createTempFile(this.getClass().getName(), "");
        write(expectedSmallBody, smallFile, forName("UTF-8"));

        postResponse = client1.addFile(smallFile, ImmutableMap.of("X-Metadata-Content-Type", "text/plain"));

        assertEquals(postResponse.getStatusCode(), 200);
        assertTrue(postResponse.getResponseBody().contains("\"result\":{\"id\""));

        Result smallFileResult = firstServerInjector.getInstance(JsonUtils.class).getFromJson(postResponse.getResponseBody());

        sleep(100);

        getResponse = client1.getFile(smallFileResult.getIdLong(), 0);
        assertResponseOk(getResponse, expectedSmallBody, "text/plain");

        sleep(100);

        // add slave node
        startServer2(10000);

        sleep(2000);

        getResponse = client2.getFile(result.getIdLong(), 0);
        assertResponseOk(getResponse, expectedBody, "text/plain");

        // check small file from an active partition has been replicated
        getResponse = client2.getFile(smallFileResult.getIdLong(), 0);
        assertResponseOk(getResponse, expectedSmallBody, "text/plain");
    }

    @Test
    public void replicateIfOnlySingleTypeIsAbsent() throws IOException, InterruptedException {
        startServer1(8);
        startServer2(8);

        File file = createTempFile(this.getClass().getName(), "");
        write("test", file, forName("UTF-8"));

        // first file
        Response postResponse1 = client1.addFile(file, ImmutableMap.of("X-Metadata-Content-Type", "text/plain"));
        assertEquals(postResponse1.getStatusCode(), 200);
        assertTrue(postResponse1.getResponseBody().contains("\"result\":{\"id\""));

        Result result1 = firstServerInjector.getInstance(JsonUtils.class).getFromJson(postResponse1.getResponseBody());

        Thread.sleep(30);

        Response getResponse1 = client1.getFile(result1.getIdLong(), 0);
        assertResponseOk(getResponse1, "test", "text/plain");

        stopServer2();

        // fill more partitions with another types of the original file
        client1.addFile(result1.getIdLong(), 1, file, ImmutableMap.of("X-Metadata-Content-Type", "text/plain"));
        client1.addFile(result1.getIdLong(), 2, file, ImmutableMap.of("X-Metadata-Content-Type", "text/plain"));
        client1.addFile(result1.getIdLong(), 3, file, ImmutableMap.of("X-Metadata-Content-Type", "text/plain"));
        client1.addFile(result1.getIdLong(), 4, file, ImmutableMap.of("X-Metadata-Content-Type", "text/plain"));
        client1.addFile(result1.getIdLong(), 5, file, ImmutableMap.of("X-Metadata-Content-Type", "text/plain"));

        // start slave again
        restartServer2(4);

        // wait for replication
        sleep(2000);

        assertEquals(client2.getFile(result1.getIdLong(), 1).getResponseBody(), "test");
        assertEquals(client2.getFile(result1.getIdLong(), 2).getResponseBody(), "test");
        assertEquals(client2.getFile(result1.getIdLong(), 3).getResponseBody(), "test");
        assertEquals(client2.getFile(result1.getIdLong(), 4).getResponseBody(), "test");
        assertEquals(client2.getFile(result1.getIdLong(), 5).getResponseBody(), "test");
    }

    @Test
    public void syncSlave() throws Exception {
        addNewSlave();

        stopServer2();

        String expectedBody = Strings.repeat("test42", 10240);

        File file = createTempFile(this.getClass().getName(), "");
        write(expectedBody, file, forName("UTF-8"));

        // add one more file
        Response postResponse = client1.addFile(file, ImmutableMap.of("X-Metadata-Content-Type", "text/plain"));

        assertEquals(postResponse.getStatusCode(), 200);
        assertTrue(postResponse.getResponseBody().contains("\"result\":{\"id\""));

        Result result = firstServerInjector.getInstance(JsonUtils.class).getFromJson(postResponse.getResponseBody());

        sleep(100);

        Response getResponse = client1.getFile(result.getIdLong(), 0);
        assertResponseOk(getResponse, expectedBody, "text/plain");

        // start slave node again
        restartServer2(10000);

        sleep(2000);

        getResponse = client2.getFile(result.getIdLong(), 0);
        assertResponseOk(getResponse, expectedBody, "text/plain");
    }

    @Test
    public void replicateOnNonexistentDisk() throws Exception {
        removeDisk(firstServerInjector, 1);
        removeDisk(secondServerInjector, 0);

        // master
        startServer1(10000);
        // slave
        startServer2(10000);

        String expectedBody = Strings.repeat("test42", 10240);

        File file = createTempFile(this.getClass().getName(), "");
        write(expectedBody, file, forName("UTF-8"));

        // add a file
        Response postResponse = client1.addFile(file, ImmutableMap.of("X-Metadata-Content-Type", "text/plain"));

        assertEquals(postResponse.getStatusCode(), 200);
        assertTrue(postResponse.getResponseBody().contains("\"result\":{\"id\""));

        Result result = firstServerInjector.getInstance(JsonUtils.class).getFromJson(postResponse.getResponseBody());

        sleep(100);

        Response getResponse = client1.getFile(result.getIdLong(), 0);
        assertResponseOk(getResponse, expectedBody, "text/plain");

        // add another file
        postResponse = client1.addFile(file, ImmutableMap.of("X-Metadata-Content-Type", "text/plain"));

        assertEquals(postResponse.getStatusCode(), 200);
        assertTrue(postResponse.getResponseBody().contains("\"result\":{\"id\""));

        result = firstServerInjector.getInstance(JsonUtils.class).getFromJson(postResponse.getResponseBody());

        sleep(500);

        getResponse = client1.getFile(result.getIdLong(), 0);
        assertResponseOk(getResponse, expectedBody, "text/plain");

        sleep(100);

        getResponse = client2.getFile(result.getIdLong(), 0);
        assertEquals(getResponse.getStatusCode(), 502);
    }

    @Test
    public void refreshOnMaster() throws Exception {
        // master
        startServer1(10000);
        // slave
        startServer2(10000);

        String expectedBody = Strings.repeat("test42", 10240);

        File file = createTempFile(this.getClass().getName(), "");
        write(expectedBody, file, forName("UTF-8"));

        // add a file
        Response postResponse = client1.addFile(file, ImmutableMap.of("X-Metadata-Content-Type", "text/plain"));

        assertEquals(postResponse.getStatusCode(), 200);
        assertTrue(postResponse.getResponseBody().contains("\"result\":{\"id\""));

        Result result = firstServerInjector.getInstance(JsonUtils.class).getFromJson(postResponse.getResponseBody());

        Response getResponse = client1.getFile(result.getIdLong(), 0);
        assertResponseOk(getResponse, expectedBody, "text/plain");

        removeDisk(firstServerInjector, 0);
        removeDisk(firstServerInjector, 1);

        firstServerInjector.getInstance(FileWriterService.class).refresh();

        // no file
        getResponse = client1.getFile(result.getIdLong(), 0);
        assertEquals(getResponse.getStatusCode(), 502);

        // ok let's add disk 0 and reconfigure server 1
        addDisk(firstServerInjector, 0);
        firstServerInjector.getInstance(FileWriterService.class).refresh();

        // master is not automatically repairable on disk failure
        getResponse = client1.getFile(result.getIdLong(), 0);
        assertEquals(getResponse.getStatusCode(), 502);

        getResponse = client2.getFile(result.getIdLong(), 0);
        assertResponseOk(getResponse, expectedBody, "text/plain");
    }

    @Test
    public void repairOnNewDiskEvent() throws Exception {
        removeDisk(firstServerInjector, 1);
        removeDisk(secondServerInjector, 0);

        // master
        startServer1(10000);
        // slave
        startServer2(10000);

        String expectedBody = Strings.repeat("test42", 10240);

        File file = createTempFile(this.getClass().getName(), "");
        write(expectedBody, file, forName("UTF-8"));

        // add a file
        Response postResponse = client1.addFile(file, ImmutableMap.of("X-Metadata-Content-Type", "text/plain"));

        assertEquals(postResponse.getStatusCode(), 200);
        assertTrue(postResponse.getResponseBody().contains("\"result\":{\"id\""));

        Result result = firstServerInjector.getInstance(JsonUtils.class).getFromJson(postResponse.getResponseBody());

        sleep(100);

        Response getResponse = client1.getFile(result.getIdLong(), 0);
        assertResponseOk(getResponse, expectedBody, "text/plain");

        // add another file
        postResponse = client1.addFile(file, ImmutableMap.of("X-Metadata-Content-Type", "text/plain"));

        assertEquals(postResponse.getStatusCode(), 200);
        assertTrue(postResponse.getResponseBody().contains("\"result\":{\"id\""));

        result = firstServerInjector.getInstance(JsonUtils.class).getFromJson(postResponse.getResponseBody());

        sleep(500);

        getResponse = client1.getFile(result.getIdLong(), 0);
        assertResponseOk(getResponse, expectedBody, "text/plain");

        sleep(500);

        getResponse = client2.getFile(result.getIdLong(), 0);
        assertEquals(getResponse.getStatusCode(), 502);

        // ok let's add disk and reconfigure server 2
        addDisk(secondServerInjector, 0);
        secondServerInjector.getInstance(FileWriterService.class).refresh();

        sleep(500);

        getResponse = client2.getFile(result.getIdLong(), 0);
        assertResponseOk(getResponse, expectedBody, "text/plain");
    }

    @Test
    public void syncCachesWhenDeleteFile() throws Exception {
        // master
        startServer1(10000);
        // slave
        startServer2(10000);

        File file = createTempFile(this.getClass().getName(), "");
        write("test", file, forName("UTF-8"));

        Response postResponse = client1.addFile(file, ImmutableMap.of("X-Metadata-Content-Type", "text/plain"));
        assertEquals(postResponse.getStatusCode(), 200);
        assertTrue(postResponse.getResponseBody().contains("\"result\":{\"id\""));

        Result result = firstServerInjector.getInstance(JsonUtils.class).getFromJson(postResponse.getResponseBody());

        Thread.sleep(30);

        Response getResponse = client1.getFile(result.getIdLong(), 0);
        assertResponseOk(getResponse, "test", "text/plain");

        getResponse = client2.getFile(result.getIdLong(), 0);
        assertResponseOk(getResponse, "test", "text/plain");

        Response deleteResponse = client1.deleteFile(result.getIdLong(), firstServerInjector.getInstance(ServerConfiguration.class).getApiToken());
        assertEquals(deleteResponse.getStatusCode(), 200);

        getResponse = client1.getFile(result.getIdLong(), 0);
        assertEquals(getResponse.getStatusCode(), 410);

        getResponse = client2.getFile(result.getIdLong(), 0);
        assertEquals(getResponse.getStatusCode(), 410);
    }

    @Test
    public void replicateFromSlave() throws IOException, InterruptedException {
        // master
        startServer1(10000);
        // slave
        startServer2(10000);

        String expectedBody = Strings.repeat("test42", 10240);

        File file = createTempFile(this.getClass().getName(), "");
        write(expectedBody, file, forName("UTF-8"));

        List<Result> fileIds = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            Response postResponse = client1.addFile(file, ImmutableMap.of("X-Metadata-Content-Type", "text/plain"));
            assertEquals(postResponse.getStatusCode(), 200);
            assertTrue(postResponse.getResponseBody().contains("\"result\":{\"id\""));
            fileIds.add(firstServerInjector.getInstance(JsonUtils.class).getFromJson(postResponse.getResponseBody()));
        }

        Thread.sleep(30);

        // corrupt data on master
        removeDisk(firstServerInjector, 0);
        removeDisk(firstServerInjector, 1);

        // start another slave
        startServer3(10000);

        RepairDiskRequest request = new RepairDiskRequest();
        request.setToken(thirdServerInjector.getInstance(ServerConfiguration.class).getApiToken());

        // repair from the second slave (change 50%/50%) and check files
        await().forever().pollInterval(ONE_HUNDRED_MILLISECONDS).until(
                () -> {
                    client3.repair(request);
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException ignored) {
                    }

                    // check only two files, that were laid out to the zero partition.
                    return fileIds.stream().limit(2).map(
                            result -> {
                                try {
                                    assertResponseOk(client3.getFile(result.getIdLong(), 0), expectedBody, "text/plain");
                                    return true;
                                } catch (AssertionError e) {
                                    return false;
                                }
                            }
                    )
                            .filter(v -> v)
                            .count() == 2;
                }
        );
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

    @Override
    protected Set<Module> getFirstInjectorModules() {
        return ImmutableSet.of(new FirstServerRootModule(), new ServerModule(), new MetricModule());
    }

    @Override
    protected Set<Module> getSecondInjectorModules() {
        return ImmutableSet.of(new SecondServerRootModule(), new ServerModule(), new MetricModule());
    }

    @Override
    protected Set<Module> getSecondRestartedModules() {
        return ImmutableSet.of(new SecondRestartedServerRootModule(), new ServerModule(), new MetricModule());
    }

    @Override
    protected Set<Module> getThirdInjectorModules() {
        return ImmutableSet.of(new ThirdServerRootModule(), new ServerModule(), new MetricModule());
    }

    @BeforeMethod(dependsOnMethods = "createInjectors")
    private void startServer() throws Exception {
        prepareDataDirectory(firstServerInjector);
        prepareDataDirectory(secondServerInjector);
        prepareDataDirectory(thirdServerInjector);

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

        client3 = new BlobKeeperClientImpl(
                thirdServerInjector.getInstance(ObjectMapper.class),
                thirdServerInjector.getInstance(ServerConfiguration.class).getBaseUrl()
        );
        client3.startAsync();
        client3.awaitRunning();
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

    private void startServer3(long fileMaxSize) {
        FileConfiguration fileConfiguration = thirdServerInjector.getInstance(FileConfiguration.class);
        fileConfiguration.setMaxFileSize(fileMaxSize);
        server3 = thirdServerInjector.getInstance(BlobKeeperServer.class);
        server3.startAsync();
        server3.awaitRunning();
    }

    private void restartServer2(long fileMaxSize) {
        FileConfiguration fileConfiguration = secondRestartedServerInjector.getInstance(FileConfiguration.class);
        fileConfiguration.setMaxFileSize(fileMaxSize);
        restartedServer2 = secondRestartedServerInjector.getInstance(BlobKeeperServer.class);
        restartedServer2.startAsync();
        restartedServer2.awaitRunning();
    }

    private void stopServer1() {
        server1 = firstServerInjector.getInstance(BlobKeeperServer.class);
        server1.stopAsync();
        server1.awaitTerminated();
    }

    private void stopServer2() {
        server2 = secondServerInjector.getInstance(BlobKeeperServer.class);
        server2.stopAsync();
        server2.awaitTerminated();
    }

    private void stopServer3() {
        server3 = thirdServerInjector.getInstance(BlobKeeperServer.class);
        server3.stopAsync();
        server3.awaitTerminated();
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

        if (null != server3) {
            server3.stopAsync();
            server3.awaitTerminated();
        }

        client1.stopAsync();
        client2.stopAsync();
        client3.stopAsync();
    }
}
