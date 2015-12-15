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

import io.blobkeeper.common.util.MerkleTree;
import io.blobkeeper.index.dao.PartitionDao;
import io.blobkeeper.index.domain.Partition;
import io.blobkeeper.index.domain.PartitionState;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Singleton
public class PartitionServiceImpl implements PartitionService {
    private static final Logger log = LoggerFactory.getLogger(PartitionServiceImpl.class);

    private final ConcurrentMap<Integer, Partition> disksToPartitions = new ConcurrentHashMap<>();

    @Inject
    private PartitionDao partitionDAO;

    @Inject
    private DiskService diskService;

    @Override
    public Partition getActivePartition(int disk) {
        return disksToPartitions.get(disk);
    }

    public void setActive(@NotNull Partition partition) {
        log.info("Active partition {}", partition);

        disksToPartitions.put(partition.getDisk(), partition);
        partitionDAO.add(partition);
    }

    @Override
    public void updateCrc(@NotNull Partition partition) {
        partitionDAO.updateCrc(partition);
    }

    @Override
    public void clearActive() {
        disksToPartitions.clear();
    }

    @Override
    public Partition getLastPartition(int disk) {
        return partitionDAO.getLastPartition(disk);
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
    public void updateState(@NotNull Partition partition) {
        partitionDAO.updateState(partition);
    }

    @Override
    public void delete(@NotNull Partition partition) {
        partitionDAO.delete(partition);
    }
}
