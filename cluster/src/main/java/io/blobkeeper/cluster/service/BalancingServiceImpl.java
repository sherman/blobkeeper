package io.blobkeeper.cluster.service;

/*
 * Copyright (C) 2016-2017 by Denis M. Gabaydulin
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
import io.blobkeeper.file.configuration.FileConfiguration;
import io.blobkeeper.file.service.DiskService;
import io.blobkeeper.file.service.PartitionService;
import io.blobkeeper.index.domain.DiskIndexElt;
import io.blobkeeper.index.domain.IndexElt;
import io.blobkeeper.index.domain.Partition;
import io.blobkeeper.index.service.IndexService;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.blobkeeper.common.util.GuavaCollectors.toImmutableMap;
import static io.blobkeeper.index.domain.PartitionState.*;
import static java.util.Comparator.comparing;
import static java.util.function.Function.identity;

@Singleton
public class BalancingServiceImpl implements BalancingService {
    private static final Logger log = LoggerFactory.getLogger(BalancingServiceImpl.class);

    @Inject
    private PartitionService partitionService;

    @Inject
    private DiskService diskService;

    @Inject
    private IndexService indexService;

    @Inject
    private FileConfiguration fileConfiguration;

    @Override
    public void balance(int disk) {
        Map<Integer, Integer> disksToPartitionsToMove = getMovePartitions();

        log.debug("Balancing data: {}", disksToPartitionsToMove);

        // balance single partition
        getDstDisk(disksToPartitionsToMove, disk).ifPresent(
                dstDisk -> {
                    partitionService.getFirstPartition(disk).ifPresent(
                            src -> {
                                // create a new partition on a destination disk
                                Partition dst = partitionService.getNextActivePartition(dstDisk);
                                movePartition(src, dst);
                            }
                    );
                }
        );

        // continue a process of rebalancing for partitions which were started earlier
        partitionService.getRebalancingStartedPartitions().stream()
                .map(src -> partitionService.getById(src.getDisk(), src.getId()))
                .filter(src -> src.getState() == NEW || src.getState() == REBALANCING)
                .forEach(
                        src -> {
                            // get a destination partition
                            partitionService.getDestination(src).ifPresent(
                                    dst -> movePartition(src, dst)
                            );
                        }
                );

        partitionService.getPartitions(disk, DATA_MOVED).forEach(
                movedPartition -> partitionService.getDestination(movedPartition).ifPresent(
                        dst -> {
                            log.info("Update a moved partition index, src {} dst {}", movedPartition, dst);

                            indexService.getListByPartition(movedPartition).forEach(
                                    file -> indexService.move(file, new DiskIndexElt(dst, file.getOffset(), file.getLength()))
                            );

                            handleEmptyPartition(movedPartition);
                        }
                )
        );

    }

    @NotNull
    @Override
    public Map<Integer, Integer> getMovePartitions() {
        Map<Integer, Integer> disksToPartitions = diskService.getDisks().stream()
                .collect(toImmutableMap(identity(), disk -> partitionService.getPartitions(disk).size()));

        if (disksToPartitions.isEmpty()) {
            return ImmutableMap.of();
        }

        int nodes = disksToPartitions.keySet().size();

        int totalPartitions = getTotalPartitions(disksToPartitions);

        int maxPartitionsPerNode = getMaxPartitionsPerNode(totalPartitions, nodes);

        log.info("Total nodes {}, total partitions {}, maxPartitionsPerNode: {}", nodes, totalPartitions, maxPartitionsPerNode);

        return disksToPartitions.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, diskToPartitions -> Math.max(diskToPartitions.getValue() - maxPartitionsPerNode - 1, 0)));
    }

    private void handleEmptyPartition(Partition partition) {
        checkArgument(partition.getState() == DATA_MOVED, "Partition state DATA_MOVED is expected!");

        try {
            log.info("Partition {} {} is going to be deleted, state = DATA_MOVED", partition.getDisk(), partition.getId());

            List<IndexElt> elts = indexService.getLiveListByPartition(partition);

            if (elts.isEmpty()) {
                log.info("No live elements are left in the partition {}", partition);

                if (!trySetDeletedState(partition)) {
                    log.warn("The state was changed, actual {}", partitionService.getById(partition.getDisk(), partition.getId()));
                }
            } else {
                log.warn("Something strange has happen, partition {} is not empty", partition);
            }
        } catch (Exception e) {
            log.error("Can't copy a file", e);
        }
    }

    private boolean trySetDeletedState(Partition partition) {
        partition.setState(DELETED);
        return partitionService.tryUpdateState(partition, DATA_MOVED);
    }

    private int getTotalPartitions(Map<Integer, Integer> disksToPartitions) {
        return disksToPartitions.values().stream()
                .mapToInt(v -> v)
                .sum();
    }

    private int getMaxPartitionsPerNode(int totalPartitions, int nodes) {
        return totalPartitions / nodes;
    }

    private void movePartition(Partition src, Partition dst) {
        // a first operation of a moving partition process
        partitionService.move(src, dst);

        if (partitionService.tryStartRebalancing(src)) {
            diskService.copyPartition(src, dst);

            // TODO: call copy partition on the cluster

            if (!partitionService.tryFinishRebalancing(src)) {
                log.warn("The state was changed, actual {}", partitionService.getById(src.getDisk(), src.getId()));
            }
        } else {
            log.warn("The state was changed, actual {}", partitionService.getById(src.getDisk(), src.getId()));
        }
    }

    private Optional<Integer> getDstDisk(Map<Integer, Integer> disksToPartitions, int srcDisk) {
        return disksToPartitions.entrySet().stream()
                .min(comparing(Map.Entry::getValue))
                .map(Map.Entry::getKey)
                .filter(toDisk -> toDisk != srcDisk);
    }
}
