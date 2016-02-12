package io.blobkeeper.cluster.service;

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
import io.blobkeeper.common.util.MerkleTree;
import io.blobkeeper.file.configuration.FileConfiguration;
import io.blobkeeper.file.domain.CompactionFile;
import io.blobkeeper.file.domain.ReplicationFile;
import io.blobkeeper.file.domain.StorageFile;
import io.blobkeeper.file.domain.TransferFile;
import io.blobkeeper.file.service.BaseFileTest;
import io.blobkeeper.file.service.CompactionQueue;
import io.blobkeeper.file.service.FileStorage;
import io.blobkeeper.file.service.PartitionService;
import io.blobkeeper.file.util.FileUtils;
import io.blobkeeper.index.domain.DiskIndexElt;
import io.blobkeeper.index.domain.IndexElt;
import io.blobkeeper.index.domain.Partition;
import io.blobkeeper.index.domain.PartitionState;
import io.blobkeeper.index.service.IndexService;
import io.blobkeeper.index.util.IndexUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;

import static com.jayway.awaitility.Awaitility.await;
import static com.jayway.awaitility.Duration.FIVE_HUNDRED_MILLISECONDS;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;

@Guice(modules = {RootModule.class})
public class CompactionServiceTest extends BaseFileTest {
    private static final Logger log = LoggerFactory.getLogger(CompactionServiceTest.class);

    @Inject
    private FileStorage fileStorage;

    @Inject
    private IndexService indexService;

    @Inject
    private IdGeneratorService generatorService;

    @Inject
    private FileConfiguration fileConfiguration;

    @Inject
    private CompactionService compactionService;

    @Inject
    private CompactionQueue compactionQueue;

    @Inject
    private IndexUtils indexUtils;

    @Inject
    private PartitionService partitionService;

    @Inject
    private ClusterMembershipService clusterMembershipService;

    @Test
    public void compaction() {
        Long fileId1 = generatorService.generate(1);

        StorageFile file1 = new StorageFile.StorageFileBuilder()
                .id(fileId1)
                .type(0)
                .name("test")
                .data(Strings.repeat("1234", 10).getBytes())
                .metadata(ImmutableMultimap.<String, String>of())
                .build();

        ReplicationFile replicationFile = fileStorage.addFile(0, file1);

        Long fileId2 = generatorService.generate(1);

        StorageFile file2 = new StorageFile.StorageFileBuilder()
                .id(fileId2)
                .type(0)
                .name("test")
                .data(Strings.repeat("1234", 2).getBytes())
                .metadata(ImmutableMultimap.<String, String>of())
                .build();

        fileStorage.addFile(0, file2);

        Partition partition = new Partition(0, 0);
        MerkleTree tree = indexUtils.buildMerkleTree(partition);
        partition.setTree(tree);

        partitionService.updateTree(partition);

        assertEquals(indexService.getById(fileId1, 0).getDiskIndexElt(), replicationFile.getIndex());

        indexService.delete(indexService.getById(fileId1, 0));

        assertTrue(FileUtils.getFilePathByPartition(fileConfiguration, partition).exists());

        await().forever().pollInterval(FIVE_HUNDRED_MILLISECONDS).until(
                () -> {
                    log.trace("Waiting for compaction");
                    return !compactionQueue.isEmpty();
                });

        // move live file to another partition
        CompactionFile compactionFile = compactionQueue.take();
        assertEquals(compactionFile.getId(), indexService.getById(fileId2, 0).getId());

        IndexElt elt = indexService.getById(compactionFile.getId(), compactionFile.getType());
        DiskIndexElt from = elt.getDiskIndexElt();
        DiskIndexElt to = new DiskIndexElt(new Partition(0, 1), 0, elt.getLength());

        fileStorage.copyFile(new TransferFile(from, to));
        indexService.move(elt, to);

        await().forever().pollInterval(FIVE_HUNDRED_MILLISECONDS).until(
                () -> {
                    log.trace("Waiting for resource cleanup");
                    return !FileUtils.getFilePathByPartition(fileConfiguration, partition).exists();
                });
    }

    @Test
    public void compactionCompletedForDeletingPartitions() {
        Long fileId1 = generatorService.generate(1);

        StorageFile file1 = new StorageFile.StorageFileBuilder()
                .id(fileId1)
                .type(0)
                .name("test")
                .data(Strings.repeat("1234", 10).getBytes())
                .metadata(ImmutableMultimap.<String, String>of())
                .build();

        ReplicationFile replicationFile1 = fileStorage.addFile(0, file1);

        Partition partition = new Partition(0, 0);
        partition.setState(PartitionState.DELETING);
        MerkleTree tree = indexUtils.buildMerkleTree(partition);
        partition.setTree(tree);

        partitionService.updateState(partition);
        partitionService.updateTree(partition);

        assertEquals(indexService.getById(fileId1, 0).getDiskIndexElt(), replicationFile1.getIndex());

        assertTrue(FileUtils.getFilePathByPartition(fileConfiguration, partition).exists());

        await().forever().pollInterval(FIVE_HUNDRED_MILLISECONDS).until(
                () -> {
                    log.trace("Waiting for compaction");
                    return !compactionQueue.isEmpty();
                });

        // move live file to another partition
        CompactionFile compactionFile = compactionQueue.take();
        assertEquals(compactionFile.getId(), indexService.getById(fileId1, 0).getId());

        IndexElt elt = indexService.getById(compactionFile.getId(), compactionFile.getType());
        DiskIndexElt from = elt.getDiskIndexElt();
        DiskIndexElt to = new DiskIndexElt(new Partition(0, 1), 0, elt.getLength());

        fileStorage.copyFile(new TransferFile(from, to));
        indexService.move(elt, to);

        await().forever().pollInterval(FIVE_HUNDRED_MILLISECONDS).until(
                () -> {
                    log.trace("Waiting for resource cleanup");
                    return !FileUtils.getFilePathByPartition(fileConfiguration, partition).exists();
                });
    }

    @BeforeMethod(dependsOnMethods = {"deleteFiles"})
    private void start() throws InterruptedException {
        clusterMembershipService.start("node1");

        indexService.clear();
        fileStorage.start();
        compactionService.start();
    }

    @AfterMethod
    private void stop() throws InterruptedException {
        compactionService.stop();
        fileStorage.stop();

        clusterMembershipService.stop();
    }
}