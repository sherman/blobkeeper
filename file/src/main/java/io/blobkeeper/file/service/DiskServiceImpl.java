package io.blobkeeper.file.service;

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

import com.google.common.collect.ImmutableList;
import io.blobkeeper.common.util.MemoizingSupplier;
import io.blobkeeper.common.util.MerkleTree;
import io.blobkeeper.file.configuration.FileConfiguration;
import io.blobkeeper.file.domain.Disk;
import io.blobkeeper.file.domain.File;
import io.blobkeeper.file.util.DiskStatistic;
import io.blobkeeper.index.domain.Partition;
import io.blobkeeper.index.service.IndexService;
import io.blobkeeper.index.util.IndexUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.blobkeeper.common.util.GuavaCollectors.toImmutableList;
import static io.blobkeeper.common.util.Maps.atomicPut;
import static io.blobkeeper.common.util.Suppliers.memoize;
import static io.blobkeeper.common.util.Utils.throwingMerger;
import static io.blobkeeper.file.util.FileUtils.getCrc;
import static io.blobkeeper.file.util.FileUtils.getOrCreateFile;
import static io.blobkeeper.index.domain.PartitionState.NEW;
import static java.lang.System.currentTimeMillis;
import static java.util.Optional.ofNullable;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

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

    @Inject
    private DiskStatistic diskStatistic;

    private volatile ConcurrentMap<Integer, Disk> activeDisks = new ConcurrentHashMap<>();

    // TODO: refactor to Partition type key
    private final ConcurrentMap<Integer, Supplier<ConcurrentMap<Integer, MemoizingSupplier<File>>>> partitionsToFiles = new ConcurrentHashMap<>();

    private final Random random = new Random();

    @Override
    public void openOnStart() {
        updateDisks();
    }

    @Override
    public void closeOnStop() {
        getActiveDisks().values().stream()
                .forEach(disk -> {
                    try {
                        closeDisk(disk);
                    } catch (Exception e) {
                        log.error("Can't close disk", e);
                    }
                });

        closeStaledFiles();

        checkArgument(partitionsToFiles.isEmpty(), "You have unclosed files!");
        checkArgument(getActiveDisks().isEmpty(), "You have unclosed active disks!");
    }

    @Override
    public void refresh() {
        getRemovedDisks().forEach(this::closeDisk);
        getAddedDisks().forEach(
                disk -> {
                    if (null == getActiveDisks().putIfAbsent(disk.getId(), disk)) {
                        log.info("New disk {} added", disk);
                    }
                }
        );
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
    public WritablePartition getWritablePartition(int diskId, long length) {
        createNextWriterIfRequired(diskId);

        Disk disk = getActiveDisks().get(diskId);

        return new WritablePartition(disk, disk.getActivePartition().incrementOffset(length));
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
    public boolean isDiskFull(int disk) {
        return partitionService.getPartitions(disk, NEW).size() >= fileConfiguration.getDiskConfiguration(disk).getMaxParts();
    }

    @Override
    public void updateErrors(int diskId) {
        ofNullable(getActiveDisks().get(diskId)).ifPresent(
                disk -> {
                    AtomicInteger errors = disk.getErrors();
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
        );
    }

    @Override
    public void resetErrors(int diskId) {
        ofNullable(getActiveDisks().get(diskId))
                .ifPresent(Disk::resetErrors);
    }

    private void createNextWriterIfRequired(int diskId) {
        boolean diskIsFull = isDiskFull(diskId);
        if (diskIsFull) {
            log.error("Disk {} is full!", diskId);
            diskStatistic.onDiskIsFullError(diskId);
            throw new IllegalArgumentException();
        }

        try {
            if (isFileReachedMaximum(diskId)) {
                long createWriterTime = currentTimeMillis();
                createNextWriter(diskId);
                log.trace("Create writer time is {}", currentTimeMillis() - createWriterTime);

            }
        } catch (Exception e) {
            log.error("Can't create next writer", e);
            throw e;
        }
    }

    private void createNextWriter(int diskId) {
        Disk disk = getActiveDisks().get(diskId);
        updateCrc(disk);
        updateMerkleTree(disk);
        closeCurrentWriter(disk);

        Disk.Builder diskBuilder = new Disk.Builder(diskId)
                .setWritable(!isDiskFull(diskId));

        createActivePartition(diskBuilder, disk.getActivePartition().getId() + 1);

        log.info("Create next writable partition {} for disk {}", diskBuilder.getActivePartition().getId(), diskId);

        Disk newDisk = diskBuilder.build();
        partitionService.setActive(newDisk.getActivePartition());
        getActiveDisks().put(diskId, newDisk);
    }

    private boolean isFileReachedMaximum(int disk) {
        return partitionService.getActivePartition(disk).getOffset() >= fileConfiguration.getMaxFileSize();
    }

    private void updateCrc(Disk disk) {
        Partition partition = disk.getActivePartition();
        checkNotNull(partition, "Active partition is required!");

        partition.setCrc(getCrc(disk.getWriter()));
        partitionService.updateCrc(partition);
    }

    private void updateMerkleTree(Disk disk) {
        Partition partition = disk.getActivePartition();
        checkNotNull(partition, "Active partition is required!");

        MerkleTree tree = indexUtils.buildMerkleTree(partition);
        partition.setTree(tree);

        partitionService.updateTree(partition);
    }

    @Override
    public void updateDisks() {
        activeDisks = fileListService.getDisks().stream()
                .map(disk -> {
                    try {
                        return createDisk(disk);
                    } catch (Exception e) {
                        log.error("Can't create disk {}", disk, e);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(toMap(Disk::getId, identity(), throwingMerger(), ConcurrentHashMap::new));

        log.info("Disks updated {}", activeDisks);
    }

    private Disk createDisk(int disIdk) {
        log.info("Create writer for disk {}", disIdk);

        Disk.Builder diskBuilder = new Disk.Builder(disIdk)
                .setWritable(!isDiskFull(disIdk));

        try {
            openActiveFile(diskBuilder);
        } catch (Exception e) {
            if (e.getCause() instanceof IOException) {
                log.error("Can't open active partition on disk {}", disIdk);
                // FIXME: proper return type
            }
            throw e;
        }

        // no active partition found
        if (diskBuilder.getActivePartition() == null) {
            createActivePartition(diskBuilder, 0);
        }

        Disk disk = diskBuilder.build();
        partitionService.setActive(disk.getActivePartition());
        return disk;
    }

    @Override
    public List<Integer> getDisks() {
        return ImmutableList.copyOf(getActiveDisks().keySet());
    }

    @Override
    public List<Disk> getRemovedDisks() {
        List<Integer> current = fileListService.getDisks();

        return getActiveDisks().values().stream()
                .filter(disk -> !current.contains(disk.getId()))
                .collect(toImmutableList());
    }

    @Override
    public List<Disk> getAddedDisks() {
        List<Integer> current = fileListService.getDisks();

        return current.stream()
                .filter(disk -> !getActiveDisks().keySet().contains(disk))
                .map(disk -> {
                    try {
                        return createDisk(disk);
                    } catch (Exception e) {
                        log.error("Can't create disk {}", disk, e);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(toImmutableList());
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

    @Override
    public Optional<Disk> get(int disk) {
        return ofNullable(getActiveDisks().get(disk));
    }

    @NotNull
    @Override
    public Map<Integer, Disk> getActiveDisks() {
        return activeDisks;
    }

    private void openActiveFile(Disk.Builder diskBuilder) {
        log.info("Open active file for disk {}", diskBuilder.getId());
        Partition activePartition = partitionService.getLastPartition(diskBuilder.getId());

        if (null != activePartition) {
            activePartition.setOffset(indexUtils.getOffset(indexService.getListByPartition(activePartition)));

            File partitionFile = getOrCreateFile(fileConfiguration, activePartition);
            diskBuilder
                    .setWriter(partitionFile)
                    .setActivePartition(activePartition);

            log.info("Active partition found {} for disk {}", activePartition, diskBuilder.getId());
        } else {
            log.info("No active partition found for disk {}", diskBuilder.getId());
        }
    }

    private void createActivePartition(Disk.Builder diskBuilder, int id) {
        log.info("Creating active partition {} for disk {}", id, diskBuilder.getId());
        Partition partition = new Partition(diskBuilder.getId(), id);

        partition.setOffset(indexUtils.getOffset(indexService.getListByPartition(partition)));

        try {
            diskBuilder
                    .setWriter(getOrCreateFile(fileConfiguration, partition))
                    .setActivePartition(partition);

            diskStatistic.onCreatePartition(diskBuilder.getId());
        } catch (Exception e) {
            log.error("Fatal error. Can't create writer", e);
            // TODO: really close all partitions?
            closeDisk(diskBuilder.build());
        }
    }

    private void closeDisk(Disk disk) {
        log.info("Close disk {}", disk);

        closeCurrentWriter(disk);
        closeDiskPartitions(disk.getId());
        removeDisk(disk);
    }

    private void removeDisk(@NotNull Disk disk) {
        getActiveDisks().remove(disk.getId());
    }

    private void closeStaledFiles() {
        for (int disk : partitionsToFiles.keySet()) {
            try {
                closeDiskPartitions(disk);
            } catch (Exception e) {
                log.error("Can't close disk", e);
            }
        }
    }

    private void closeCurrentWriter(Disk disk) {
        try {
            ofNullable(disk.getWriter())
                    .ifPresent(File::close);
        } catch (Exception ignored) {
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
