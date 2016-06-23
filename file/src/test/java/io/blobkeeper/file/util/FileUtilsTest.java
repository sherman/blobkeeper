package io.blobkeeper.file.util;

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

import com.datastax.driver.core.utils.Bytes;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import com.google.common.io.Files;
import io.blobkeeper.common.configuration.MetricModule;
import io.blobkeeper.common.configuration.RootModule;
import io.blobkeeper.common.util.MerkleTree;
import io.blobkeeper.file.configuration.FileConfiguration;
import io.blobkeeper.file.configuration.FileModule;
import io.blobkeeper.file.domain.File;
import io.blobkeeper.file.service.BaseFileTest;
import io.blobkeeper.file.service.FileListService;
import io.blobkeeper.file.service.PartitionService;
import io.blobkeeper.index.configuration.IndexConfiguration;
import io.blobkeeper.index.domain.IndexElt;
import io.blobkeeper.index.domain.Partition;
import io.blobkeeper.index.service.IndexService;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.io.IOException;
import java.nio.ByteBuffer;

import static io.blobkeeper.file.util.FileUtils.getFilePathByPartition;
import static org.joda.time.DateTime.now;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;

@Guice(modules = {RootModule.class, MetricModule.class, FileModule.class})
public class FileUtilsTest extends BaseFileTest {

    @Inject
    private IndexService indexService;

    @Inject
    private FileListService fileListService;

    @Inject
    private PartitionService partitionService;

    @Inject
    private FileConfiguration fileConfiguration;

    @Inject
    private IndexConfiguration indexConfiguration;

    @Test
    public void buildMerkleTree() throws IOException {
        java.io.File dataFile1 = getFilePathByPartition(configuration, 0, 0);
        Files.touch(dataFile1);

        partitionService.setActive(new Partition(0, 0));

        File file = fileListService.getFile(0, 0);

        byte[] data = new byte[128];
        for (int i = 0; i < 128; i++) {
            data[i] = 0x1;
        }

        file.getFileChannel().write(ByteBuffer.wrap(data), 0);

        IndexElt expected = new IndexElt.IndexEltBuilder()
                .id(214803434770010112L)
                .type(1)
                .partition(new Partition(0, 0))
                .offset(0L)
                .length(128L)
                .metadata(ImmutableMap.of("key", "value"))
                .crc(FileUtils.getCrc(data))
                .build();

        indexService.add(expected);

        MerkleTree tree = FileUtils.buildMerkleTree(indexService, file, new Partition(0, 0));
        assertEquals(tree.getLeafNodes().get(0).getRange(), Range.openClosed(214803434770010111L, 214803434770010112L));
        assertEquals(tree.getLeafNodes().get(0).getHash(), new byte[]{2, -5, 34, -107, -6, 0, 16, 0, 0, 0, 0, 1});

        file.close();
    }

    @Test
    public void getPercentOfDeleted() {
        IndexElt expected = new IndexElt.IndexEltBuilder()
                .id(214803434770010112L)
                .type(1)
                .partition(new Partition(0, 0))
                .offset(0L)
                .length(1L)
                .deleted(true)
                .metadata(ImmutableMap.of("key", "value"))
                .crc(42L)
                .updated(now(UTC).minusSeconds(indexConfiguration.getGcGraceTime() + 1).getMillis())
                .build();

        indexService.add(expected);

        assertEquals(FileUtils.getPercentOfDeleted(fileConfiguration, indexService, new Partition(0, 0)), 1);
    }

    @Test
    public void readFile() throws IOException {
        java.io.File dataFile1 = getFilePathByPartition(configuration, 0, 0);
        Files.touch(dataFile1);

        partitionService.setActive(new Partition(0, 0));

        File file = fileListService.getFile(0, 0);

        byte[] data = new byte[]{0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9};
        file.getFileChannel().write(ByteBuffer.wrap(data), 0);

        assertEquals(Bytes.getArray(FileUtils.readFile(file, 0, 1))[0], 0x0);
        assertEquals(Bytes.getArray(FileUtils.readFile(file, 3, 1))[0], 0x3);
        assertEquals(Bytes.getArray(FileUtils.readFile(file, 0, 1))[0], 0x0);

        file.close();
    }

    @BeforeMethod
    private void clear() {
        indexService.clear();
    }
}
