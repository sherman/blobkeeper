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
 *
 *
 * Gets the list of the blob files
 */

import com.google.common.primitives.Ints;
import io.blobkeeper.file.configuration.FileConfiguration;
import io.blobkeeper.file.domain.File;
import io.blobkeeper.index.domain.Partition;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.blobkeeper.common.util.GuavaCollectors.toImmutableList;
import static io.blobkeeper.file.util.FileUtils.getDiskPathByDisk;
import static io.blobkeeper.file.util.FileUtils.getFilePathByPartition;
import static org.apache.commons.io.filefilter.DirectoryFileFilter.DIRECTORY;

@Singleton
public class FileListServiceImpl implements FileListService {
    private static final Logger log = LoggerFactory.getLogger(FileListServiceImpl.class);

    @Inject
    private FileConfiguration configuration;

    @Inject
    private PartitionService partitionService;

    @Override
    public List<File> getFiles(int disk, @NotNull String pattern) {
        java.io.File filePath = getDiskPathByDisk(configuration, disk);
        checkArgument(filePath.exists(), "Base disk path must be exists");

        return Arrays
                .stream(filePath.list(new SuffixFileFilter(pattern)))
                .map(blobFileName -> getFile(filePath, blobFileName))
                .collect(toImmutableList());
    }

    @Override
    public List<File> getFiles(@NotNull String pattern) {
        return getDisks()
                .stream()
                .map(disk -> getFiles(disk, pattern))
                .flatMap(List::stream)
                .collect(toImmutableList());
    }

    @Nullable
    @Override
    public File getFile(int disk, int partition) {
        Partition partitionInfo = partitionService.getById(disk, partition);
        if (partitionInfo == null) {
            log.error("Disk:Partition not found {}:{}", disk, partition);
            return null;
        }

        log.info("Getting partition {}", partitionInfo);

        java.io.File filePath = new java.io.File(configuration.getBasePath());
        checkArgument(filePath.exists(), "Base path must be exists");

        java.io.File file = getFilePathByPartition(configuration, partitionInfo);
        if (!file.exists()) {
            return null;
        } else {
            return new File(file);
        }
    }

    @Override
    public List<Integer> getDisks() {
        java.io.File filePath = new java.io.File(configuration.getBasePath());
        checkArgument(filePath.exists(), "Base path must be exists");

        return Arrays
                .stream(filePath.list(DIRECTORY))
                .map(this::parseDisk)
                .filter(disk -> disk != null)
                .collect(toImmutableList());
    }

    @Override
    public void deleteFile(int disk, int partition) {
        java.io.File filePath = new java.io.File(configuration.getBasePath());
        checkArgument(filePath.exists(), "Base path must be exists");

        java.io.File file = getFilePathByPartition(configuration, new Partition(disk, partition));
        if (file.exists()) {
            if (!file.delete()) {
                log.error("Can't delete file " + filePath);
            }
        }
    }

    private Integer parseDisk(String diskName) {
        log.info("Disk found {}", diskName);
        return Ints.tryParse(diskName);
    }

    private File getFile(java.io.File filePath, String blobFileName) {
        java.io.File file = new java.io.File(FilenameUtils.concat(filePath.getAbsolutePath(), blobFileName));
        return new File(file);
    }

}
