package io.blobkeeper.file.service;

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
 *
 *
 * Gets the list of the blob files
 */

import com.google.common.base.Preconditions;
import io.blobkeeper.file.domain.Disk;
import io.blobkeeper.index.dao.PartitionDao;
import io.blobkeeper.index.domain.Partition;
import io.blobkeeper.index.domain.PartitionState;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.Optional;

import static io.blobkeeper.index.domain.PartitionState.DATA_MOVED;
import static io.blobkeeper.index.domain.PartitionState.NEW;
import static io.blobkeeper.index.domain.PartitionState.REBALANCING;
import static java.util.Optional.ofNullable;

@Singleton
public class PartitionServiceImpl implements PartitionService {
    private static final Logger log = LoggerFactory.getLogger(PartitionServiceImpl.class);

    @Inject
    private PartitionDao partitionDAO;

    @Inject
    private DiskService diskService;

    // TODO: return Optional<Partition> ?
    @Override
    public Partition getActivePartition(int disk) {
        return diskService.get(disk)
                .map(Disk::getActivePartition)
                .orElse(null);
    }

    public void setActive(@NotNull Partition partition) {
        log.info("Active partition {}", partition);
        partitionDAO.add(partition);
    }

    @Override
    public void updateCrc(@NotNull Partition partition) {
        partitionDAO.updateCrc(partition);
    }

    @Override
    public Partition getLastPartition(int disk) {
        return partitionDAO.getLastPartition(disk);
    }

    @Override
    public Optional<Partition> getFirstPartition(int disk) {
        return partitionDAO.getFirstPartition(disk);
    }

    @NotNull
    @Override
    public List<Partition> getPartitions(int disk) {
        return partitionDAO.getPartitions(disk);
    }

    @NotNull
    @Override
    public List<Partition> getPartitions(int disk, @NotNull PartitionState state) {
        return partitionDAO.getPartitions(disk, state);
    }

    @Override
    public Partition getById(int disk, int id) {
        return partitionDAO.getById(disk, id);
    }

    @Override
    public void updateTree(@NotNull Partition partition) {
        partitionDAO.updateTree(partition);
    }

    @Override
    public boolean tryUpdateState(@NotNull Partition partition, @NotNull PartitionState expected) {
        return partitionDAO.tryUpdateState(partition, expected);
    }

    @Override
    public boolean tryStartRebalancing(@NotNull Partition partition) {
        if (partition.getState() == REBALANCING) {
            return true;
        }

        Preconditions.checkArgument(partition.getState() == NEW, "NEW state is required!");

        PartitionState oldState = partition.getState();
        partition.setState(REBALANCING);
        return partitionDAO.tryUpdateState(partition, oldState);
    }

    @Override
    public boolean tryFinishRebalancing(@NotNull Partition partition) {
        if (partition.getState() == DATA_MOVED) {
            return true;
        }

        Preconditions.checkArgument(partition.getState() == REBALANCING, "REBALANCING state is required!");

        PartitionState oldState = partition.getState();
        partition.setState(DATA_MOVED);
        return partitionDAO.tryUpdateState(partition, oldState);
    }

    @Override
    public boolean tryDelete(@NotNull Partition partition) {
        return partitionDAO.tryDelete(partition);
    }

    @Override
    public synchronized Partition getNextActivePartition(int disk) {
        Partition partition = getLastPartition(disk);
        Partition nextActive;
        if (partition == null) {
            nextActive = new Partition(disk, 0);
        } else {
            nextActive = new Partition(disk, partition.getId() + 1);
        }

        setActive(nextActive);
        return nextActive;
    }

    @Override
    public void move(@NotNull Partition from, @NotNull Partition to) {
        partitionDAO.move(from, to);
    }

    @Override
    public Optional<Partition> getDestination(@NotNull Partition movedPartition) {
        return partitionDAO.getDestination(movedPartition);
    }

    @NotNull
    @Override
    public List<Partition> getRebalancingStartedPartitions() {
        return partitionDAO.getRebalancingStartedPartitions();
    }
}
