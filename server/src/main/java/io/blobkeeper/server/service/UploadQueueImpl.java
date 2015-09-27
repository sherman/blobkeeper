package io.blobkeeper.server.service;

import io.blobkeeper.file.domain.StorageFile;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.slf4j.LoggerFactory.getLogger;

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

@Singleton
public class UploadQueueImpl implements UploadQueue {
    private static final Logger log = getLogger(UploadQueueImpl.class);
    private final BlockingQueue<StorageFile> filesToSave = new ArrayBlockingQueue<>(1048576);

    public boolean offer(@NotNull StorageFile file) {
        if (log.isTraceEnabled()) {
            log.trace("Putting file to the queue");
        }
        checkNotNull(file, "File is required!");
        return filesToSave.offer(file);
    }

    @NotNull
    public StorageFile take() {
        try {
            return filesToSave.take();
        } catch (InterruptedException e) {
            log.error("Can't get file from the queue", e);
            throw new IllegalStateException(e);
        }
    }

    @Override
    public boolean isEmpty() {
        return filesToSave.isEmpty();
    }
}
