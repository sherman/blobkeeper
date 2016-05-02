package io.blobkeeper.server.service;

/*
 * Copyright (C) 2016 by Denis M. Gabaydulin
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

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMultimap;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.blobkeeper.cluster.service.ClusterMembershipService;
import io.blobkeeper.common.configuration.RootModule;
import io.blobkeeper.common.service.IdGeneratorService;
import io.blobkeeper.file.configuration.FileConfiguration;
import io.blobkeeper.file.domain.StorageFile;
import io.blobkeeper.file.service.BaseFileTest;
import io.blobkeeper.index.domain.IndexTempElt;
import io.blobkeeper.index.service.IndexService;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static com.google.common.io.Files.write;
import static com.jayway.awaitility.Awaitility.await;
import static com.jayway.awaitility.Duration.FIVE_HUNDRED_MILLISECONDS;
import static org.joda.time.DateTime.now;
import static org.joda.time.DateTimeZone.UTC;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Guice(modules = {RootModule.class, FileWriterServiceTest.Mocks.class})
public class FileWriterServiceTest extends BaseFileTest {
    private static final Logger log = LoggerFactory.getLogger(FileWriterServiceTest.class);

    @Inject
    private IndexService indexService;

    @Inject
    private IdGeneratorService generatorService;

    @Inject
    private FileConfiguration fileConfiguration;

    @Inject
    private FileWriterService fileWriterService;

    @Inject
    private ClusterMembershipService clusterMembershipService;

    @Inject
    private UploadQueue uploadQueue;

    @Test
    public void repair() throws IOException, InterruptedException {
        when(clusterMembershipService.isMaster()).thenReturn(true);

        Set<Long> ids = new HashSet<>(128);

        for (int i = 0; i < 128; i++) {
            java.io.File file = new java.io.File(fileConfiguration.getUploadPath() + "1234" + i);
            write("4242", file, Charsets.UTF_8);

            Long fileId = generatorService.generate(1);
            ids.add(fileId);

            StorageFile storageFile = new StorageFile.StorageFileBuilder()
                    .id(fileId)
                    .type(0)
                    .name("test")
                    .file(file)
                    .headers(ImmutableMultimap.<String, String>of())
                    .build();

            IndexTempElt indexElt = new IndexTempElt.IndexTempEltBuilder()
                    .id(storageFile.getId())
                    .type(storageFile.getType())
                    .created(now(UTC).getMillis())
                    .metadata(storageFile.getMetadata())
                    .file(storageFile.getFile().getAbsolutePath())
                    .build();

            indexService.add(indexElt);
        }

        // restore
        fileWriterService.start();

        await().forever().pollInterval(FIVE_HUNDRED_MILLISECONDS).until(
                () -> {
                    log.trace("Waiting for upload queue");
                    return uploadQueue.isEmpty();
                });

        assertUploadDirectoryIsEmpty();
        assertTrue(indexService.getTempIndexList(1024).isEmpty());

        long foundElt = ids.stream()
                .map(id -> indexService.getById(id, 0))
                .filter(Objects::nonNull)
                .count();

        assertEquals(foundElt, 128);
    }

    @BeforeMethod(dependsOnMethods = {"deleteFiles"})
    private void start() throws InterruptedException {
        indexService.clear();
    }

    @AfterMethod
    private void stop() throws InterruptedException {
        fileWriterService.stop();
    }

    private void assertUploadDirectoryIsEmpty() {
        assertEquals(new File(fileConfiguration.getUploadPath())
                .list().length, 0);
    }

    public static class Mocks extends AbstractModule {
        @Provides
        @Singleton
        ClusterMembershipService clusterMembershipService() {
            return Mockito.mock(ClusterMembershipService.class);
        }

        @Override
        protected void configure() {
        }
    }
}
