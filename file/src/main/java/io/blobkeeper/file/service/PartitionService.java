package io.blobkeeper.file.service;

import com.google.inject.ImplementedBy;
import io.blobkeeper.common.util.MerkleTree;
import io.blobkeeper.index.domain.Partition;
import io.blobkeeper.index.domain.PartitionState;
import org.jetbrains.annotations.NotNull;

import java.util.List;

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

@ImplementedBy(PartitionServiceImpl.class)
public interface PartitionService {

    Partition getActivePartition(int disk);

    void setActive(@NotNull Partition partition);

    void updateCrc(@NotNull Partition partition);

    void clearActive();

    Partition getLastPartition(int disk);

    @NotNull
    List<Partition> getPartitions(int disk);

    @NotNull
    List<Partition> getPartitions(int disk, @NotNull PartitionState state);

    Partition getById(int disk, int id);

    void updateTree(@NotNull Partition partition);

    void updateState(@NotNull Partition partition);

    void delete(@NotNull Partition partition);
}
