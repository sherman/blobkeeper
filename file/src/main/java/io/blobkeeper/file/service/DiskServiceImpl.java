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

import com.google.common.collect.ImmutableList;
import io.blobkeeper.common.util.GuavaCollectors;
import io.blobkeeper.common.util.MemoizingSupplier;
import io.blobkeeper.common.util.MerkleTree;
import io.blobkeeper.file.configuration.FileConfiguration;
import io.blobkeeper.file.domain.File;
import io.blobkeeper.index.domain.Partition;
import io.blobkeeper.index.service.IndexService;
import io.blobkeeper.index.util.IndexUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.blobkeeper.common.util.Maps.atomicPut;
import static io.blobkeeper.common.util.Suppliers.memoize;
import static io.blobkeeper.file.util.FileUtils.getCrc;
import static io.blobkeeper.file.util.FileUtils.getOrCreateFile;
import static java.lang.System.currentTimeMillis;
import static java.util.Optional.ofNullable;

@Singleton
public class DiskServiceImpl implements DiskService {
    private static final Logger log = LoggerFactory.getLogger(DiskServiceImpl.class);

    @Inject
    private FileListService fileListService;

    @Inject
    private PartitionService partitionService;

    @Inject
    private FileConfiguration fileConfiguration;

    @Inject
    private IndexService indexService;

    @Inject
    private IndexUtils indexUtils;

    private volatile List<Integer> disks = ImmutableList.of();

    private final ConcurrentMap<Integer, File> fileWriters = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, AtomicInteger> disksToErrors = new ConcurrentHashMap<>();
    // TODO: refactor to Partition type key
    private final ConcurrentMap<Integer, Supplier<ConcurrentMap<Integer, MemoizingSupplier<File>>>> partitionsToFiles = new ConcurrentHashMap<>();

    private final Random random = new Random();

    @Override
    public void openOnStart() {
        updateDiskCount();

        for (int disk : getDisks()) {
            try {
                createDiskWriter(disk);
            } catch (Exception e) {
                log.error("Can't start disk", e);
            }
        }
    }

    @Override
    public void closeOnStop() {
        for (int disk : getDisks()) {
            try {
                closeDisk(disk);
            } catch (Exception e) {
                log.error("Can't close disk", e);
            }
        }

        closeStaledFiles();

        partitionService.clearActive();

        checkArgument(partitionsToFiles.isEmpty(), "You have unclosed files!");
        checkArgument(fileWriters.isEmpty(), "You have unclosed file writers!");
    }

    @Override
    public void refresh() {
        getRemovedDisks().forEach(this::closeDisk);
        getAddedDisks().forEach(this::createDiskWriter);
        updateDiskCount();
    }

    @Override
    public File getWriter(int disk) {
        checkNotNull(fileWriters.get(disk), "File writer must be created!");

        return fileWriters.get(disk);
    }

    @Override
    public File getFile(@NotNull Partition partition) {
        if (partition.getId() < 0) {
            log.error("Can't find partition");
            throw new IllegalArgumentException("Can't find partition");
        }

        if (partition.getDisk() < 0) {
            log.error("Can't find disk");
            throw new IllegalArgumentException("Can't find disk");
        }

        try {
            return _getFile(partition);
        } catch (Exception e) {
            if (e.getCause() instanceof IOException) {
                updateErrors(partition.getDisk());
            }
            throw e;
        }
    }

    @Override
    public int getRandomDisk() {
        List<Integer> list = getDisks();
        if (list.isEmpty()) {
            throw new IllegalArgumentException("No disk available!");
        }

        return list.get(random.nextInt(list.size()));
    }

    @Override
    public void createNextWriterIfRequired(int disk) {
        try {
            if (isFileReachedMaximum(disk)) {
                long createWriterTime = currentTimeMillis();
                createNextWriter(disk);
                log.trace("Create writer time is {}", currentTimeMillis() - createWriterTime);

            }
        } catch (Exception e) {
            log.error("Can't create next writer", e);
        }
    }

    @Override
    public void updateErrors(int disk) {
        AtomicInteger errors = disksToErrors.get(disk);
        if (null != errors) {
            int diskErrors = errors.incrementAndGet();
            if (diskErrors >= fileConfiguration.getMaxDiskWriteErrors()) {
                log.info(
                        "Disk {} seem to be broken (max errors {}) and will be disabled",
                        disk,
                        fileConfiguration.getMaxDiskWriteErrors()
                );

                closeDisk(disk);
            }
        }
    }

    @Override
    public void resetErrors(int disk) {
        AtomicInteger errors = atomicPut(disksToErrors, disk, new AtomicInteger());
        errors.set(0);
    }

    private void createNextWriter(int disk) {
        updateCrc(disk);
        updateMerkleTree(disk);
        closeCurrentWriter(disk);
        createActivePartition(disk, partitionService.getActivePartition(disk).getId() + 1);
    }

    private boolean isFileReachedMaximum(int disk) {
        return partitionService.getActivePartition(disk).getOffset() >= fileConfiguration.getMaxFileSize();
    }

    private void updateCrc(int disk) {
        Partition partition = partitionService.getActivePartition(disk);
        checkNotNull(partition, "Active partition is required!");

        partition.setCrc(getCrc(fileWriters.get(disk)));
        partitionService.updateCrc(partition);
    }

    private void updateMerkleTree(int disk) {
        Partition partition = partitionService.getActivePartition(disk);
        checkNotNull(partition, "Active partition is required!");

        MerkleTree tree = indexUtils.buildMerkleTree(partition);
        partition.setTree(tree);

        partitionService.updateTree(partition);
    }

    @Override
    public void updateDiskCount() {
        disks = fileListService.getDisks();
    }

    @Override
    public List<Integer> getDisks() {
        return disks;
    }

    @Override
    public List<Integer> getRemovedDisks() {
        List<Integer> current = fileListService.getDisks();

        return disks.stream()
                .filter(disk -> !current.contains(disk))
                .collect(GuavaCollectors.toImmutableList());
    }

    @Override
    public List<Integer> getAddedDisks() {
        List<Integer> current = fileListService.getDisks();

        return current.stream()
                .filter(disk -> !disks.contains(disk))
                .collect(GuavaCollectors.toImmutableList());
    }

    @Override
    public void removeDisk(int removeDisk) {
        disks = disks.stream()
                .filter(disk -> disk != removeDisk)
                .collect(GuavaCollectors.toImmutableList());
    }

    @Override
    public void deleteFile(@NotNull Partition partition) {
        ofNullable(partitionsToFiles.get(partition.getDisk())).ifPresent(
                supplier -> ofNullable(supplier.get().get(partition.getId())).ifPresent(
                        fSupplier -> {
                            if (fSupplier.isInit()) {
                                try {
                                    fSupplier.get().close();
                                } catch (Exception e) {
                                    log.error("Can't delete file", e);
                                }
                            }

                            supplier.get().remove(partition.getId());
                        }
                )
        );

        fileListService.deleteFile(partition.getDisk(), partition.getId());
    }

    private void createDiskWriter(int disk) {
        log.info("Create writer for disk {}", disk);

        resetErrors(disk);

        try {
            openActiveFile(disk);
        } catch (Exception e) {
            if (e.getCause() instanceof IOException) {
                log.error("Can't open active partition on disk {}", disk);
                return;
            }
            throw e;
        }

        // no active partition found
        if (null == partitionService.getActivePartition(disk)) {
            createActivePartition(disk, 0);
        }
    }

    private void openActiveFile(int disk) {
        log.info("Open active file for disk {}", disk);
        Partition activePartition = partitionService.getLastPartition(disk);

        if (null != activePartition) {
            File partitionFile = getOrCreateFile(fileConfiguration, activePartition);
            fileWriters.put(disk, partitionFile);

            activePartition.setOffset(indexUtils.getOffset(indexService.getListByPartition(activePartition)));
            partitionService.setActive(activePartition);
            log.info("Active partition found {} for disk {}", activePartition, disk);
        } else {
            log.info("No active partition found for disk {}", disk);
        }
    }

    private void createActivePartition(int disk, int id) {
        log.info("Creating active partition {} for disk {}", id, disk);
        Partition partition = new Partition(disk, id);

        partition.setOffset(indexUtils.getOffset(indexService.getListByPartition(partition)));

        try {
            File dataFile = getOrCreateFile(fileConfiguration, partition);
            fileWriters.put(disk, dataFile);

            partitionService.setActive(partition);
        } catch (Exception e) {
            log.error("Fatal error. Can't create writer", e);
            closeDisk(disk);
        }
    }

    private void closeDisk(int disk) {
        log.info("Close disk {}", disk);

        closeCurrentWriter(disk);
        closeDiskPartitions(disk);
        removeErrors(disk);
        removeDisk(disk);
    }

    private void removeErrors(int disk) {
        disksToErrors.remove(disk);
    }

    private void closeStaledFiles() {
        for (int disk : partitionsToFiles.keySet()) {
            try {
                closeDisk(disk);
            } catch (Exception e) {
                log.error("Can't close disk", e);
            }
        }

        for (int disk : fileWriters.keySet()) {
            try {
                closeDisk(disk);
            } catch (Exception e) {
                log.error("Can't close disk", e);
            }
        }

        for (int disk : disksToErrors.keySet()) {
            try {
                closeDisk(disk);
            } catch (Exception e) {
                log.error("Can't close disk", e);
            }
        }
    }

    private void closeCurrentWriter(int disk) {
        try {
            if (null != fileWriters.get(disk)) {
                fileWriters.get(disk).close();
            }
        } catch (Exception ignored) {
        } finally {
            if (null != fileWriters.get(disk)) {
                fileWriters.remove(disk);
            }
        }
    }

    private void closeDiskPartitions(int disk) {
        Supplier<ConcurrentMap<Integer, MemoizingSupplier<File>>> diskPartitions = partitionsToFiles.get(disk);
        if (null != diskPartitions) {
            diskPartitions.get().values().stream()
                    .filter(MemoizingSupplier::isInit)
                    .forEach(thunk -> {
                        try {
                            thunk.get().close();
                        } catch (Exception e) {
                            log.error("Can't close file", e);
                        }
                    });
        }

        partitionsToFiles.remove(disk);
    }

    private File _getFile(Partition partition) {
        Supplier<ConcurrentMap<Integer, MemoizingSupplier<File>>> diskPartitions = atomicPut(
                partitionsToFiles,
                partition.getDisk(),
                memoize(ConcurrentHashMap::new)
        );

        MemoizingSupplier<File> fileSupplier = atomicPut(
                diskPartitions.get(),
                partition.getId(),
                memoize(() -> getOrCreateFile(fileConfiguration, partition))
        );

        return fileSupplier.get();
    }
}
