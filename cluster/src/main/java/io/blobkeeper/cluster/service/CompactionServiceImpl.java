package io.blobkeeper.cluster.service;

/*
 * Copyright (C) 2015-2017 by Denis M. Gabaydulin
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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.blobkeeper.common.util.ResultWrapper;
import io.blobkeeper.file.configuration.FileConfiguration;
import io.blobkeeper.file.domain.StorageFile;
import io.blobkeeper.file.service.DiskService;
import io.blobkeeper.file.service.PartitionService;
import io.blobkeeper.file.service.WriterTaskQueue;
import io.blobkeeper.file.util.FileUtils;
import io.blobkeeper.index.domain.IndexElt;
import io.blobkeeper.index.domain.Partition;
import io.blobkeeper.index.domain.PartitionState;
import io.blobkeeper.index.service.IndexService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.Maps.immutableEntry;
import static io.blobkeeper.common.util.Streams.parallelize;
import static io.blobkeeper.index.domain.PartitionState.DELETED;
import static io.blobkeeper.index.domain.PartitionState.DELETING;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;

@Singleton
public class CompactionServiceImpl implements CompactionService {
    private static final Logger log = LoggerFactory.getLogger(CompactionServiceImpl.class);

    @Inject
    private FileConfiguration fileConfiguration;

    @Inject
    private IndexService indexService;

    @Inject
    private PartitionService partitionService;

    @Inject
    private DiskService diskService;

    @Inject
    private ClusterMembershipService membershipService;

    @Inject
    private WriterTaskQueue compactionQueue;

    private final AtomicInteger compactions = new AtomicInteger();
    private final AtomicInteger finalizations = new AtomicInteger();

    private final ScheduledExecutorService compactionExecutor = newScheduledThreadPool(
            16,
            new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("CompactionWorker-%d")
                    .build()
    );

    private final Runnable deletePartitionFinalizer = new DeletedPartitionFinalizer();

    private final Runnable compactionWorker = new CompactionWorker();

    @Override
    public void start() {
        log.info("Compaction service started");

        compactionExecutor.scheduleWithFixedDelay(
                compactionWorker,
                fileConfiguration.getCompactionWorkerDelaySeconds(),
                fileConfiguration.getCompactionWorkerDelaySeconds(),
                SECONDS
        );

        compactionExecutor.scheduleWithFixedDelay(
                deletePartitionFinalizer,
                fileConfiguration.getCompactionFinalizerDelaySeconds(),
                fileConfiguration.getCompactionFinalizerDelaySeconds(),
                SECONDS
        );
    }

    @Override
    public void stop() {
        log.info("Compaction service stopped");
    }

    @Override
    public int getFinalizations() {
        return finalizations.get();
    }

    @Override
    public int getCompactions() {
        return compactions.get();
    }

    private class DeletedPartitionFinalizer implements Runnable {
        @Override
        public void run() {
            try {
                finalizeDeletedPartitions();
            } catch (Exception e) {
                log.error("Can't clean files", e);
            } finally {
                finalizations.incrementAndGet();
            }
        }

        private void finalizeDeletedPartitions() {
            // finally delete empty partitions and free disk-space
            diskService.getDisks().stream()
                    .map(disk -> partitionService.getPartitions(disk, DELETED))
                    .flatMap(Collection::stream)
                    .forEach(partition -> {
                        try {
                            log.info(
                                    "Partition {} {} file is going to be physically deleted",
                                    partition.getDisk(),
                                    partition.getId()
                            );

                            membershipService.deletePartitionFile(partition.getDisk(), partition.getId());

                            if (!partitionService.tryDelete(partition)) {
                                throw new IllegalStateException();
                            }
                        } catch (Exception e) {
                            log.error("Can't delete partition {}", partition, e);
                        }
                    });
        }
    }

    private class CompactionWorker implements Runnable {
        @Override
        public void run() {
            try {
                if (!compactionQueue.isEmpty()) {
                    log.info("Previous compaction is in progress");
                    return;
                }

                handleDeletingPartitions();
                handleNewPartitions();
            } catch (Exception e) {
                log.error("Can't copy live files", e);
            } finally {
                compactions.incrementAndGet();
            }
        }

        private void handleDeletingPartitions() {
            // complete copy of live files in the DELETING partitions
            parallelize(diskService.getDisks(), disk -> () -> partitionService.getPartitions(disk, DELETING))
                    .filter(r -> !r.hasError())
                    .map(ResultWrapper::getResult)
                    // TOOD: parallelize the whole stage?
                    .flatMap(Collection::stream)
                    .forEach(
                            partition -> {
                                try {
                                    log.info(
                                            "Partition {} {} is going to be deleted, state = DELETING",
                                            partition.getDisk(),
                                            partition.getId()
                                    );

                                    // copy live files to a new partition
                                    moveLiveFiles(partition);
                                } catch (Exception e) {
                                    log.error("Can't copy a file", e);
                                }
                            }
                    );

        }

        private void handleNewPartitions() {
            parallelize(diskService.getDisks(), disk -> () -> partitionService.getPartitions(disk))
                    .filter(r -> !r.hasError())
                    .map(ResultWrapper::getResult)
                    // TOOD: parallelize the whole stage?
                    .flatMap(Collection::stream)
                    // for any non-active partition, merkle-tree has been built
                    .filter(partition -> partition.getTree() != null)
                    .map(partition -> immutableEntry(partition, FileUtils.getPercentOfDeleted(fileConfiguration, indexService, partition)))
                    .filter(entry -> entry.getValue() > fileConfiguration.getMinPercent())
                    .forEach(
                            entry -> {
                                try {
                                    log.info(
                                            "Partition {} {} is going to be deleted, deleted files percent is {}",
                                            entry.getKey().getDisk(),
                                            entry.getKey().getId(),
                                            entry.getValue()
                                    );

                                    // try to update partition state to DELETING
                                    if (tryStartDeletingPartition(entry.getKey())) {
                                        // move live files to a new partition
                                        moveLiveFiles(entry.getKey());
                                    } else {
                                        log.warn("The state was changed, actual {}", partitionService.getById(entry.getKey().getDisk(), entry.getKey().getId()));
                                    }
                                } catch (Exception e) {
                                    log.error("Can't copy a file", e);
                                }
                            }
                    );
        }

        private boolean tryStartDeletingPartition(Partition partition) {
            PartitionState oldState = partition.getState();
            partition.setState(DELETING);
            return partitionService.tryUpdateState(partition, oldState);
        }

        private boolean trySetDeletedState(Partition partition) {
            partition.setState(DELETED);
            return partitionService.tryUpdateState(partition, DELETING);
        }

        private void moveLiveFiles(Partition partition) {
            List<IndexElt> elts = indexService.getLiveListByPartition(partition);

            if (elts.isEmpty()) {
                log.info("No live elements are left in the partition {}", partition);
                if (!trySetDeletedState(partition)) {
                    log.warn("The state was changed, actual {}", partitionService.getById(partition.getDisk(), partition.getId()));
                }
            } else {
                elts.forEach(elt -> compactionQueue.offer(
                        new StorageFile.CompactionFileBuilder()
                                .id(elt.getId())
                                .type(elt.getType())
                                .build()));
            }
        }
    }
}
