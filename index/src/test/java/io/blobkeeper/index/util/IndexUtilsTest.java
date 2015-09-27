package io.blobkeeper.index.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import io.blobkeeper.common.configuration.RootModule;
import io.blobkeeper.common.service.IdGeneratorService;
import io.blobkeeper.common.util.MerkleTree;
import io.blobkeeper.index.dao.IndexDao;
import io.blobkeeper.index.domain.IndexElt;
import io.blobkeeper.index.domain.Partition;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

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

@Guice(modules = RootModule.class)
public class IndexUtilsTest {
    @Inject
    private IndexDao indexDao;

    @Inject
    private IndexUtils indexUtils;

    @Inject
    private IdGeneratorService generatorService;

    @Test
    public void buildMerkleTree() {
        long newId = generatorService.generate(1);

        Partition partition = new Partition(42, 42);

        assertTrue(indexDao.getListById(newId).isEmpty());
        assertNull(indexDao.getById(newId, 1));

        IndexElt expected = new IndexElt.IndexEltBuilder()
                .id(newId)
                .type(1)
                .partition(partition)
                .offset(0L)
                .length(128L)
                .metadata(ImmutableMap.of("key", "value"))
                .crc(42L)
                .build();

        indexDao.add(expected);

        MerkleTree merkleTree = indexUtils.buildMerkleTree(partition);
        assertEquals(merkleTree.getLeafNodes().size(), 1);
    }

    @Test
    public void difference() {
        Partition partition1 = new Partition(42, 42);
        IndexElt expected = new IndexElt.IndexEltBuilder()
                .id(42L)
                .type(1)
                .partition(partition1)
                .offset(0L)
                .length(128L)
                .metadata(ImmutableMap.of("key", "value"))
                .crc(42L)
                .build();

        indexDao.add(expected);

        Partition partition2 = new Partition(42, 43);
        IndexElt expected2 = new IndexElt.IndexEltBuilder()
                .id(43L)
                .type(1)
                .partition(partition2)
                .offset(0L)
                .length(128L)
                .metadata(ImmutableMap.of("key", "value"))
                .crc(42L)
                .build();

        indexDao.add(expected2);

        MerkleTree merkleTree1 = indexUtils.buildMerkleTree(partition1);
        MerkleTree merkleTree2 = indexUtils.buildMerkleTree(partition2);

        assertEquals(MerkleTree.difference(merkleTree1, merkleTree2).get(0).getRange(), Range.openClosed(42L, 43L));
    }

    @BeforeMethod
    private void clear() {
        indexDao.clear();
    }
}
