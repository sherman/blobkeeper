package io.blobkeeper.file.service;

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

import com.google.common.io.Files;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.blobkeeper.common.configuration.MetricModule;
import io.blobkeeper.common.configuration.RootModule;
import io.blobkeeper.file.domain.File;
import io.blobkeeper.index.dao.IndexDao;
import io.blobkeeper.index.dao.PartitionDao;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static io.blobkeeper.file.util.FileUtils.getDiskPathByDisk;
import static io.blobkeeper.file.util.FileUtils.getFilePathByPartition;
import static java.util.Collections.sort;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Guice(modules = {RootModule.class, FileListServiceTest.Mocks.class, MetricModule.class})
public class FileListServiceTest extends BaseFileTest {
    private static final Logger log = LoggerFactory.getLogger(FileListServiceTest.class);

    @Inject
    private FileListService fileListService;

    @Test
    public void getDataFilesEmpty() {
        List<File> dataFiles = new ArrayList<>(fileListService.getFiles(0, ".data"));
        assertTrue(dataFiles.isEmpty());
    }

    @Test
    public void getDataFilesByDisk() throws IOException {
        java.io.File dataFile1 = getFilePathByPartition(configuration, 0, 1);
        java.io.File dataFile2 = getFilePathByPartition(configuration, 0, 2);
        Files.touch(dataFile1);
        Files.touch(dataFile2);

        List<File> dataFiles = new ArrayList<>(fileListService.getFiles(0, ".data"));
        assertEquals(dataFiles.size(), 2);
        sort(dataFiles);
        assertEquals(dataFiles.get(0).getName(), "1.data");
        assertEquals(dataFiles.get(1).getName(), "2.data");
    }

    @Test
    public void getDataFiles() throws IOException {
        java.io.File dataFile1 = getFilePathByPartition(configuration, 0, 1);
        java.io.File dataFile2 = getFilePathByPartition(configuration, 1, 2);
        Files.touch(dataFile1);
        Files.touch(dataFile2);

        List<File> dataFiles = new ArrayList<>(fileListService.getFiles(".data"));
        assertEquals(dataFiles.size(), 2);
        sort(dataFiles);
        assertEquals(dataFiles.get(0).getName(), "1.data");
        assertEquals(dataFiles.get(1).getName(), "2.data");
    }

    @Test
    public void getEmptyDisks() {
        java.io.File disk1 = getDiskPathByDisk(configuration, 0);
        java.io.File disk2 = getDiskPathByDisk(configuration, 1);
        disk1.delete();
        disk2.delete();

        int disks = fileListService.getDisks().size();
        assertEquals(disks, 0);
    }

    @Test
    public void getDisks() {
        int disks = fileListService.getDisks().size();
        assertEquals(disks, 2);
    }

    public static class Mocks extends AbstractModule {
        @Provides
        @Singleton
        PartitionDao partitionDao() {
            return Mockito.mock(PartitionDao.class);
        }

        @Provides
        @Singleton
        IndexDao indexDao() {
            return Mockito.mock(IndexDao.class);
        }

        @Override
        protected void configure() {
        }
    }
}
