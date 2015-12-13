package io.blobkeeper.file.util;

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
import com.google.common.io.Files;
import io.blobkeeper.common.configuration.RootModule;
import io.blobkeeper.common.util.MerkleTree;
import io.blobkeeper.file.configuration.FileConfiguration;
import io.blobkeeper.file.domain.File;
import io.blobkeeper.file.service.BaseFileTest;
import io.blobkeeper.file.service.FileListService;
import io.blobkeeper.file.service.PartitionService;
import io.blobkeeper.index.domain.IndexElt;
import io.blobkeeper.index.domain.Partition;
import io.blobkeeper.index.service.IndexService;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.io.IOException;
import java.nio.ByteBuffer;

import static io.blobkeeper.file.util.FileUtils.getFilePathByPartition;
import static org.testng.Assert.assertEquals;

@Guice(modules = {RootModule.class})
public class FileUtilsTest extends BaseFileTest {

    @Inject
    private IndexService indexService;

    @Inject
    private FileListService fileListService;

    @Inject
    private PartitionService partitionService;

    @Inject
    private FileConfiguration fileConfiguration;

    @Test
    public void buildMerkleTree() throws IOException {
        java.io.File dataFile1 = getFilePathByPartition(configuration, 0, 0);
        Files.touch(dataFile1);

        partitionService.setActive(new Partition(0, 0));

        File file = fileListService.getFile(0, 0);

        IndexElt expected = new IndexElt.IndexEltBuilder()
                .id(214803434770010112L)
                .type(1)
                .partition(new Partition(0, 0))
                .offset(0L)
                .length(128L)
                .metadata(ImmutableMap.of("key", "value"))
                .crc(42L)
                .build();

        indexService.add(expected);

        byte[] data = new byte[(int) expected.getLength()];
        for (int i = 0; i < expected.getLength(); i++) {
            data[i] = 0x1;
        }

        file.getFileChannel().write(ByteBuffer.wrap(data), expected.getOffset());

        MerkleTree tree = FileUtils.buildMerkleTree(indexService, file, new Partition(0, 0));
        assertEquals(tree.getLeafNodes().get(0).getRange(), Range.openClosed(214803434770010111L, 214803434770010112L));
        assertEquals(tree.getLeafNodes().get(0).getHash(), new byte[]{2, -5, 34, -107, -6, 0, 16, 0});
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
                .build();

        indexService.add(expected);

        assertEquals(FileUtils.getPercentOfDeleted(fileConfiguration, indexService, new Partition(0, 0)), 1);

    }

    @BeforeMethod
    private void clear() {
        indexService.clear();
    }
}
