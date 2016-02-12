package io.blobkeeper.index.dao;

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

import com.google.inject.ImplementedBy;
import io.blobkeeper.common.util.MerkleTree;
import io.blobkeeper.index.domain.Partition;
import io.blobkeeper.index.domain.PartitionState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.util.List;

@ImplementedBy(PartitionDaoImpl.class)
public interface PartitionDao {
    Partition getLastPartition(int disk);

    void add(@NotNull Partition partition);

    void updateCrc(@NotNull Partition partition);

    @NotNull
    List<Partition> getPartitions(int disk);

    @NotNull
    List<Partition> getPartitions(int disk, @NotNull PartitionState state);

    Partition getById(int disk, int id);

    void updateTree(@NotNull Partition partition);

    void updateState(@NotNull Partition partition);

    @TestOnly
    void clear();

    void delete(@NotNull Partition partition);
}
