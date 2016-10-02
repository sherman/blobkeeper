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

import com.google.inject.ImplementedBy;
import io.blobkeeper.file.domain.Disk;
import io.blobkeeper.file.domain.File;
import io.blobkeeper.index.domain.Partition;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@ImplementedBy(DiskServiceImpl.class)
public interface DiskService {
    /**
     * Start/stop writers
     */
    void openOnStart();

    void closeOnStop();

    void refresh();

    File getFile(@NotNull Partition partition);

    /**
     * Must be used from single thread per disk
     */
    WritablePartition getWritablePartition(int disk, long length);

    int getRandomDisk();

    boolean isDiskFull(int disk);

    /**
     * Errors
     */
    void updateErrors(int diskId);

    void resetErrors(int diskId);

    /**
     * Disk methods
     */
    void updateDisks();

    List<Integer> getDisks();

    List<Disk> getRemovedDisks();

    List<Disk> getAddedDisks();

    void deleteFile(@NotNull Partition partition);

    Optional<Disk> get(int disk);

    @NotNull
    Map<Integer, Disk> getActiveDisks();

    void copyPartition(@NotNull Partition from, @NotNull Partition to);
}
