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
import com.google.common.collect.ImmutableSet;
import io.blobkeeper.common.configuration.MetricModule;
import io.blobkeeper.common.configuration.RootModule;
import io.blobkeeper.common.service.IdGeneratorService;
import io.blobkeeper.common.util.Streams;
import io.blobkeeper.file.configuration.FileConfiguration;
import io.blobkeeper.file.configuration.FileModule;
import io.blobkeeper.file.domain.ReplicationFile;
import io.blobkeeper.file.domain.StorageFile;
import io.blobkeeper.file.domain.TransferFile;
import io.blobkeeper.file.util.DiskStatistic;
import io.blobkeeper.file.util.FileUtils;
import io.blobkeeper.index.domain.DiskIndexElt;
import io.blobkeeper.index.domain.Partition;
import io.blobkeeper.index.domain.PartitionState;
import io.blobkeeper.index.service.IndexService;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static io.blobkeeper.file.util.FileUtils.readFileToString;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

@Guice(modules = {RootModule.class, MetricModule.class, FileModule.class})
public class FileStorageTest extends BaseFileTest {

    @Inject
    private FileStorage fileStorage;

    @Inject
    private IndexService indexService;

    @Inject
    private IdGeneratorService generatorService;

    @Inject
    private FileConfiguration fileConfiguration;

    @Inject
    private DiskService diskService;

    @Inject
    private DiskStatistic diskStatistic;

    @Inject
    private PartitionService partitionService;

    @Test
    public void getDiskConfiguration() {
        assertEquals(fileConfiguration.getDiskConfiguration(0).getMaxParts(), 2);
    }

    @Test
    public void addFile() {
        Long fileId = generatorService.generate(1);

        StorageFile file = new StorageFile.StorageFileBuilder()
                .id(fileId)
                .type(0)
                .name("test")
                .data(Strings.repeat("1234", 128).getBytes())
                .headers(ImmutableMultimap.<String, String>of())
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
                .headers(ImmutableMultimap.<String, String>of())
                .build();

        ReplicationFile replicationFile = fileStorage.addFile(0, file);

        assertEquals(indexService.getById(fileId, 0).getDiskIndexElt(), replicationFile.getIndex());

        java.io.File copyFile = FileUtils.getFilePathByPartition(fileConfiguration, new Partition(0, 1));
        assertFalse(copyFile.exists());

        DiskIndexElt to = new DiskIndexElt(new Partition(0, 1), 0, replicationFile.getIndex().getLength());
        TransferFile transferFile = new TransferFile(replicationFile.getIndex(), to);

        fileStorage.copyFile(transferFile);

        copyFile = FileUtils.getFilePathByPartition(fileConfiguration, new Partition(0, 1));
        assertEquals(copyFile.length(), replicationFile.getIndex().getLength());

        assertEquals(readFileToString(copyFile), Strings.repeat("1234", 128));
    }

    @Test
    public void copyMultipleFiles() {
        for (int i = 0; i < 8; i++) {
            Long fileId = generatorService.generate(1);

            StorageFile file = new StorageFile.StorageFileBuilder()
                    .id(fileId)
                    .type(0)
                    .name("test")
                    .data(Strings.repeat("" + i, 8).getBytes())
                    .headers(ImmutableMultimap.<String, String>of())
                    .build();

            ReplicationFile replicationFile = fileStorage.addFile(0, file);

            DiskIndexElt to = new DiskIndexElt(new Partition(0, 1), 0, replicationFile.getIndex().getLength());
            TransferFile transferFile = new TransferFile(replicationFile.getIndex(), to);

            fileStorage.copyFile(transferFile);

            java.io.File copyFile = FileUtils.getFilePathByPartition(fileConfiguration, new Partition(0, 1));
            assertEquals(
                    readFileToString(copyFile).substring((int) transferFile.getTo().getOffset(), (int) transferFile.getTo().getLength()),
                    Strings.repeat("" + i, 8)
            );
        }
    }

    @Test
    public void copyFileWithIndexUpdate() throws InterruptedException {
        Long fileId = generatorService.generate(1);

        StorageFile file = new StorageFile.StorageFileBuilder()
                .id(fileId)
                .type(0)
                .name("test")
                .data(Strings.repeat("1234", 128).getBytes())
                .headers(ImmutableMultimap.<String, String>of())
                .build();

        ReplicationFile replicationFile = fileStorage.addFile(0, file);

        assertEquals(indexService.getById(fileId, 0).getDiskIndexElt(), replicationFile.getIndex());

        java.io.File copyFile = FileUtils.getFilePathByPartition(fileConfiguration, new Partition(0, 1));
        assertFalse(copyFile.exists());

        fileStorage.copyFile(0, new StorageFile.CompactionFileBuilder().id(fileId).type(0).build());

        copyFile = FileUtils.getFilePathByPartition(fileConfiguration, new Partition(0, 1));
        assertEquals(copyFile.length(), replicationFile.getIndex().getLength());

        assertEquals(indexService.getById(fileId, 0).getDiskIndexElt().getPartition(), new Partition(0, 1));
    }

    //@Test
    public void multiThreadAddOrCopy() {
        // for a test only
        fileConfiguration.getDiskConfiguration(0).setMaxParts(42);
        fileConfiguration.getDiskConfiguration(1).setMaxParts(42);

        Set<Integer> disks = ImmutableSet.of(0, 1);

        Streams.parallelize(
                disks,
                disk -> () -> {
                    List<ReplicationFile> writtenFiles = new ArrayList<>();

                    for (int i = 0; i < 64; i++) {
                        Long fileId = generatorService.generate(1);

                        StorageFile file = new StorageFile.StorageFileBuilder()
                                .id(fileId)
                                .type(0)
                                .name("test")
                                .data(Strings.repeat("1234", 8).getBytes())
                                .headers(ImmutableMultimap.<String, String>of())
                                .build();

                        writtenFiles.add(fileStorage.addFile(disk, file));
                    }

                    return writtenFiles;
                }
        )
                .flatMap(wrapper -> wrapper.getResult().stream())
                .forEach(
                        file -> {
                            ByteBuffer buffer = FileUtils.readFile(
                                    diskService.getFile(file.getIndex().getPartition()),
                                    file.getIndex().getOffset(),
                                    file.getIndex().getLength()
                            );

                            byte[] bufferBytes = new byte[buffer.remaining()];
                            buffer.get(bufferBytes);

                            assertEquals(bufferBytes, Strings.repeat("1234", 8).getBytes());
                        }
                );

        assertEquals(diskStatistic.getCreatedPartitions(0), 16);
        assertEquals(diskStatistic.getCreatedPartitions(1), 16);

        partitionService.getPartitions(0, PartitionState.NEW).forEach(
                partition -> indexService.getListByPartition(partition).forEach(
                        elt -> assertEquals(elt.getLength(), 32)
                )
        );

        // restore original
        fileConfiguration.getDiskConfiguration(0).setMaxParts(2);
        fileConfiguration.getDiskConfiguration(1).setMaxParts(2);
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
