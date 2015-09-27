package io.blobkeeper.index.dao;

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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import com.google.common.primitives.Longs;
import io.blobkeeper.common.configuration.RootModule;
import io.blobkeeper.common.util.LeafNode;
import io.blobkeeper.common.util.MerkleTree;
import io.blobkeeper.index.domain.IndexElt;
import io.blobkeeper.index.domain.Partition;
import io.blobkeeper.index.util.IndexUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;

import static com.google.common.collect.ImmutableList.of;
import static org.testng.Assert.*;
import static org.testng.Assert.assertEquals;

@Guice(modules = RootModule.class)
public class PartitionDaoTest {

    @Inject
    private PartitionDao partitionDao;

    @Inject
    private IndexDao indexDao;

    @Inject
    private IndexUtils indexUtils;

    @Test
    public void getEmpty() {
        assertNull(partitionDao.getLastPartition(42));
    }

    @Test
    public void getLastPartition() {
        assertNull(partitionDao.getLastPartition(42));

        Partition partition = new Partition(42, 42);
        partition.setCrc(42L);

        partitionDao.add(partition);

        assertEquals(partitionDao.getLastPartition(partition.getDisk()), partition);

        Partition oldPartition = new Partition(42, 1);
        oldPartition.setCrc(42L);

        partitionDao.add(oldPartition);

        assertEquals(partitionDao.getLastPartition(partition.getDisk()), partition);
    }

    @Test
    public void getPartitions() {
        assertTrue(partitionDao.getPartitions(42).isEmpty());

        Partition partition1 = new Partition(42, 42);
        partition1.setCrc(42L);

        partitionDao.add(partition1);

        Partition partition2 = new Partition(42, 43);
        partition2.setCrc(42L);

        partitionDao.add(partition2);

        assertEquals(partitionDao.getPartitions(42), of(partition2, partition1));
    }

    @Test
    public void getById() {
        assertNull(partitionDao.getById(42, 42));

        Partition partition1 = new Partition(42, 42);
        partition1.setCrc(42L);

        partitionDao.add(partition1);

        assertEquals(partitionDao.getById(partition1.getDisk(), partition1.getId()), partition1);
    }

    @Test
    public void updateCrc() {
        assertNull(partitionDao.getById(42, 42));

        Partition partition1 = new Partition(42, 42);
        partition1.setCrc(42L);

        partitionDao.add(partition1);

        assertEquals(partitionDao.getById(partition1.getDisk(), partition1.getId()), partition1);

        partition1.setCrc(44444444444L);

        partitionDao.updateCrc(partition1);

        assertEquals(partitionDao.getById(partition1.getDisk(), partition1.getId()), partition1);
    }

    @Test
    public void updateMerkleTree() {
        assertNull(partitionDao.getById(42, 42));

        Partition partition1 = new Partition(42, 42);

        partitionDao.add(partition1);

        IndexElt indexElt = new IndexElt.IndexEltBuilder()
                .id(42L)
                .type(1)
                .partition(partition1)
                .offset(0L)
                .length(128L)
                .metadata(ImmutableMap.of("key", "value"))
                .build();

        indexDao.add(indexElt);

        MerkleTree tree = indexUtils.buildMerkleTree(partition1);

        partitionDao.updateTree(partition1, tree);

        LeafNode node = tree.getLeafNodes().stream()
                .findFirst()
                .get();

        assertEquals(node.getRange(), Range.openClosed(indexElt.getId() - 1, indexElt.getId()));
        assertEquals(node.getBlocks(), 1);
        assertEquals(node.getHash(), Longs.toByteArray(indexElt.getId()));
        assertEquals(node.getLength(), indexElt.getLength());
    }

    @Test
    public void loadMerkleTree() {
        updateMerkleTree();

        Partition partition = partitionDao.getById(42, 42);
        IndexElt indexElt = indexDao.getById(42L, 1);

        MerkleTree tree = partition.getTree();

        LeafNode node = tree.getLeafNodes().stream()
                .findFirst()
                .get();

        assertEquals(node.getRange(), Range.openClosed(indexElt.getId() - 1, indexElt.getId()));
        assertEquals(node.getBlocks(), 1);
        assertEquals(node.getHash(), Longs.toByteArray(indexElt.getId()));
        assertEquals(node.getLength(), indexElt.getLength());
    }

    @BeforeMethod
    private void clear() {
        partitionDao.clear();
        indexDao.clear();
    }
}
