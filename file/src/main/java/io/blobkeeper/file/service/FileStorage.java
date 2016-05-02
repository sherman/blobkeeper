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
import io.blobkeeper.file.domain.*;
import io.blobkeeper.index.domain.IndexElt;
import org.jetbrains.annotations.NotNull;

@ImplementedBy(FileStorageImpl.class)
public interface FileStorage {

    void start();

    void stop();

    void refresh();

    /**
     * If addFile() method will be called from multiple threads, disk must be bound to a thread!
     *
     * @param storageFile to save in the storage
     * @throws java.lang.IllegalArgumentException if file has not been added to the storage
     */
    ReplicationFile addFile(int disk, @NotNull StorageFile storageFile);

    void addFile(@NotNull ReplicationFile replicationFile);

    void copyFile(@NotNull TransferFile transferFile);

    void copyFile(int disk, @NotNull CompactionFile from);

    File getFile(@NotNull IndexElt indexElt);
}
