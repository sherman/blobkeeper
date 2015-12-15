package io.blobkeeper.file.service;

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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMultimap;
import io.blobkeeper.common.configuration.RootModule;
import io.blobkeeper.common.service.IdGeneratorService;
import io.blobkeeper.file.configuration.FileConfiguration;
import io.blobkeeper.file.domain.*;
import io.blobkeeper.file.util.FileUtils;
import io.blobkeeper.index.domain.DiskIndexElt;
import io.blobkeeper.index.domain.Partition;
import io.blobkeeper.index.service.IndexService;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;

import static org.testng.AssertJUnit.assertEquals;

@Guice(modules = {RootModule.class})
public class FileStorageTest extends BaseFileTest {

    @Inject
    private FileStorage fileStorage;

    @Inject
    private IndexService indexService;

    @Inject
    private IdGeneratorService generatorService;

    @Inject
    private FileConfiguration fileConfiguration;

    @Test
    public void addFile() {
        Long fileId = generatorService.generate(1);

        StorageFile file = new StorageFile.StorageFileBuilder()
                .id(fileId)
                .type(0)
                .name("test")
                .data(Strings.repeat("1234", 128).getBytes())
                .metadata(ImmutableMultimap.<String, String>of())
                .build();

        ReplicationFile replicationFile = fileStorage.addFile(0, file);

        assertEquals(indexService.getById(fileId, 0).getDiskIndexElt(), replicationFile.getIndex());
    }

    @Test
    public void copyFile() {
        Long fileId = generatorService.generate(1);

        StorageFile file = new StorageFile.StorageFileBuilder()
                .id(fileId)
                .type(0)
                .name("test")
                .data(Strings.repeat("1234", 128).getBytes())
                .metadata(ImmutableMultimap.<String, String>of())
                .build();

        ReplicationFile replicationFile = fileStorage.addFile(0, file);

        assertEquals(indexService.getById(fileId, 0).getDiskIndexElt(), replicationFile.getIndex());

        java.io.File copyFile = FileUtils.getFilePathByPartition(fileConfiguration, new Partition(0, 1));
        assertEquals(copyFile.length(), fileConfiguration.getMaxFileSize());

        DiskIndexElt to = new DiskIndexElt(new Partition(0, 1), 0, replicationFile.getIndex().getLength());
        TransferFile transferFile = new TransferFile(replicationFile.getIndex(), to);

        fileStorage.copyFile(transferFile);

        copyFile = FileUtils.getFilePathByPartition(fileConfiguration, new Partition(0, 1));
        assertEquals(copyFile.length(), replicationFile.getIndex().getLength());
    }

    @Test
    public void copyFileWithIndexUpdate() {
        Long fileId = generatorService.generate(1);

        StorageFile file = new StorageFile.StorageFileBuilder()
                .id(fileId)
                .type(0)
                .name("test")
                .data(Strings.repeat("1234", 128).getBytes())
                .metadata(ImmutableMultimap.<String, String>of())
                .build();

        ReplicationFile replicationFile = fileStorage.addFile(0, file);

        assertEquals(indexService.getById(fileId, 0).getDiskIndexElt(), replicationFile.getIndex());

        java.io.File copyFile = FileUtils.getFilePathByPartition(fileConfiguration, new Partition(0, 1));
        assertEquals(copyFile.length(), fileConfiguration.getMaxFileSize());

        fileStorage.copyFile(0, new CompactionFile(fileId, 0));

        copyFile = FileUtils.getFilePathByPartition(fileConfiguration, new Partition(0, 1));
        assertEquals(copyFile.length(), replicationFile.getIndex().getLength());

        assertEquals(indexService.getById(fileId, 0).getDiskIndexElt().getPartition(), new Partition(0, 1));
    }

    @BeforeMethod(dependsOnMethods = {"deleteFiles"})
    private void start() throws InterruptedException {
        indexService.clear();
        fileStorage.start();
    }

    @AfterMethod
    private void stop() throws InterruptedException {
        fileStorage.stop();
    }
}
