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
import io.blobkeeper.common.configuration.RootModule;
import io.blobkeeper.common.service.IdGeneratorService;
import io.blobkeeper.index.domain.IndexElt;
import io.blobkeeper.index.domain.Partition;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;

import static com.google.common.collect.ImmutableList.of;
import static org.testng.Assert.*;

@Guice(modules = RootModule.class)
public class IndexDaoTest {

    @Inject
    private IndexDao indexDao;

    @Inject
    private IdGeneratorService generatorService;

    @Test
    public void getEmpty() {
        assertNull(indexDao.getById(42L, 0));
    }

    @Test
    public void add() {
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

        assertEquals(indexDao.getById(newId, 1), expected);
        assertEquals(indexDao.getById(newId, 1).getPartition(), expected.getPartition());
        assertEquals(indexDao.getById(newId, 1).getCrc(), expected.getCrc());
        assertEquals(indexDao.getListByPartition(partition), of(expected));
    }

    @Test
    public void partitionFilter() {
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
                .build();

        indexDao.add(expected);

        Partition anotherPartition = new Partition(41, 41);

        IndexElt notExpected = new IndexElt.IndexEltBuilder()
                .id(newId)
                .type(0)
                .partition(anotherPartition)
                .offset(0L)
                .length(128L)
                .metadata(ImmutableMap.of("key", "value"))
                .build();

        indexDao.add(notExpected);

        assertEquals(indexDao.getById(newId, 1), expected);
        assertEquals(indexDao.getById(newId, 1).getPartition(), expected.getPartition());
        assertEquals(indexDao.getListByPartition(partition), of(expected));
    }

    @Test
    public void deletedFilter() {
        partitionFilter();

        Partition partition = new Partition(42, 42);
        indexDao.getListByPartition(partition).forEach(
                elt -> indexDao.updateDelete(elt.getId(), true)
        );

        assertTrue(indexDao.getLiveListByPartition(partition).isEmpty());
    }

    @Test
    public void updateDeleted() {
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
                .build();

        indexDao.add(expected);

        assertEquals(indexDao.getById(newId, 1), expected);
        assertEquals(indexDao.getById(newId, 1).getPartition(), expected.getPartition());
        assertEquals(indexDao.getListByPartition(partition), of(expected));

        indexDao.updateDelete(newId, true);

        assertEquals(indexDao.getById(newId, 1), expected);
        assertTrue(indexDao.getById(newId, 1).isDeleted());
    }

    @BeforeMethod
    private void clear() {
        indexDao.clear();
    }
}
