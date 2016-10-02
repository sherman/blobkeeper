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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.blobkeeper.common.configuration.MetricModule;
import io.blobkeeper.common.configuration.RootModule;
import io.blobkeeper.common.service.IdGeneratorService;
import io.blobkeeper.file.configuration.FileConfiguration;
import io.blobkeeper.file.configuration.FileModule;
import io.blobkeeper.file.util.DiskStatistic;
import io.blobkeeper.file.util.FileUtils;
import io.blobkeeper.index.domain.IndexElt;
import io.blobkeeper.index.domain.Partition;
import io.blobkeeper.index.service.IndexService;
import io.blobkeeper.index.service.NoIndexRangeException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;

import static io.blobkeeper.file.util.FileUtils.writeFile;
import static org.testng.Assert.*;

@Guice(modules = {RootModule.class, MetricModule.class, FileModule.class})
public class DiskServiceTest extends BaseFileTest {

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
    public void openOnStart() {
        assertFalse(diskService.get(0).isPresent());
        assertFalse(diskService.get(1).isPresent());

        diskService.openOnStart();

        assertTrue(diskService.get(0).isPresent());
        assertTrue(diskService.get(1).isPresent());

        assertTrue(diskService.get(0).get().getWriter().getFileChannel().isOpen());
        assertTrue(diskService.get(1).get().getWriter().getFileChannel().isOpen());

        assertTrue(diskService.get(1).get().isWritable());
        assertTrue(diskService.get(1).get().isWritable());

        assertEquals(diskService.get(0).get().getActivePartition(), new Partition(0, 0));
        assertEquals(diskService.get(1).get().getActivePartition(), new Partition(1, 0));
    }

    @Test
    public void getWritablePartition() {
        diskService.openOnStart();

        WritablePartition partition = diskService.getWritablePartition(0, 42L);
        assertEquals(partition.getDisk().getActivePartition(), new Partition(0, 0));
        assertTrue(partition.getDisk().getWriter().getFileChannel().isOpen());
        assertTrue(partition.getDisk().isWritable());
        assertEquals(partition.getNextOffset(), 42L);

        // get next
        partition = diskService.getWritablePartition(0, 42L);
        assertEquals(partition.getDisk().getActivePartition(), new Partition(0, 0));
        assertTrue(partition.getDisk().getWriter().getFileChannel().isOpen());
        assertTrue(partition.getDisk().isWritable());
        assertEquals(partition.getNextOffset(), 84L);
    }

    @Test(expectedExceptions = NoIndexRangeException.class)
    public void createNextWritablePartitionFailed() {
        diskService.openOnStart();

        diskService.getWritablePartition(0, 128L);

        // get next
        WritablePartition partition = diskService.getWritablePartition(0, 42L);
        assertEquals(partition.getDisk().getActivePartition(), new Partition(0, 0));
        assertTrue(partition.getDisk().getWriter().getFileChannel().isOpen());
        assertTrue(partition.getDisk().isWritable());
        assertEquals(partition.getNextOffset(), 170L);
    }

    @Test
    public void createNextWritablePartition() {
        diskService.openOnStart();

        WritablePartition partition = diskService.getWritablePartition(0, 128L);

        IndexElt indexElt = new IndexElt.IndexEltBuilder()
                .id(generatorService.generate(1))
                .type(0)
                .partition(partition.getDisk().getActivePartition())
                .offset(partition.getNextOffset() - 128L)
                .length(128L)
                .crc(42L)
                .metadata(ImmutableMap.of())
                .build();

        indexService.add(indexElt);

        // get next
        partition = diskService.getWritablePartition(0, 42L);
        assertEquals(partition.getDisk().getActivePartition(), new Partition(0, 1));
        assertTrue(partition.getDisk().getWriter().getFileChannel().isOpen());
        assertTrue(partition.getDisk().isWritable());
        assertEquals(partition.getNextOffset(), 42L);

        assertEquals(diskService.getDisks(), ImmutableList.of(0, 1));
    }

    @Test
    public void closeDisk() {
        diskService.openOnStart();

        WritablePartition partition = diskService.getWritablePartition(0, 128L);

        IndexElt indexElt = new IndexElt.IndexEltBuilder()
                .id(generatorService.generate(1))
                .type(0)
                .partition(partition.getDisk().getActivePartition())
                .offset(partition.getNextOffset() - 128L)
                .length(128L)
                .crc(42L)
                .metadata(ImmutableMap.of())
                .build();

        indexService.add(indexElt);

        // get next
        partition = diskService.getWritablePartition(0, 42L);
        assertEquals(partition.getDisk().getActivePartition(), new Partition(0, 1));
        assertTrue(partition.getDisk().getWriter().getFileChannel().isOpen());
        assertTrue(partition.getDisk().isWritable());
        assertEquals(partition.getNextOffset(), 42L);

        assertEquals(diskService.getDisks(), ImmutableList.of(0, 1));

        assertTrue(diskService.getFile(new Partition(0, 0)).getFileChannel().isOpen());
        assertTrue(diskService.getFile(new Partition(0, 1)).getFileChannel().isOpen());

        diskService.closeOnStop();

        assertEquals(diskService.getDisks(), ImmutableList.of());
    }

    @Test
    public void closePreviousActivePartition() {
        diskService.openOnStart();

        WritablePartition partition = diskService.getWritablePartition(0, 128L);

        IndexElt indexElt = new IndexElt.IndexEltBuilder()
                .id(generatorService.generate(1))
                .type(0)
                .partition(partition.getDisk().getActivePartition())
                .offset(partition.getNextOffset() - 128L)
                .length(128L)
                .crc(42L)
                .metadata(ImmutableMap.of())
                .build();

        indexService.add(indexElt);

        // get next
        WritablePartition nextPartition = diskService.getWritablePartition(0, 42L);

        assertFalse(partition.getDisk().getWriter().getFileChannel().isOpen());
        assertTrue(nextPartition.getDisk().getWriter().getFileChannel().isOpen());

        // try to write to a closed channel
        ByteBuffer buffer = ByteBuffer.wrap(new byte[]{0x1, 0x2, 0x3, 0x4});
        try {
            partition.getDisk().getWriter().getFileChannel().write(buffer);
            fail();
        } catch (IOException e) {
            assertEquals(e.getClass(), ClosedChannelException.class);
        }
    }

    @Test
    public void copyPartition() throws IOException {
        diskService.openOnStart();

        byte[] data = new byte[]{0x1, 0x2, 0x3, 0x4};

        assertEquals(writeFile(diskService.getFile(new Partition(0, 0)), data, 0), 4);

        diskService.copyPartition(new Partition(0, 0), new Partition(1, 1));

        ByteBuffer byteBuffer = FileUtils.readFile(diskService.getFile(new Partition(1, 1)), 0, 4);
        byte[] bufferBytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(bufferBytes);

        assertEquals(bufferBytes, data);
    }

    @BeforeMethod(dependsOnMethods = {"deleteFiles"})
    private void start() throws InterruptedException {
        indexService.clear();
    }

    @AfterMethod
    private void stop() throws InterruptedException {
        diskService.closeOnStop();
    }
}
