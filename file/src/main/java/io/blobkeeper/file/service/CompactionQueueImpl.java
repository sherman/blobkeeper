package io.blobkeeper.file.service;

/*
 * Copyright (C) 2016 by Denis M. Gabaydulin
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

import com.google.inject.Singleton;
import io.blobkeeper.file.domain.CompactionFile;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static com.google.common.base.Preconditions.checkNotNull;

@Singleton
public class CompactionQueueImpl implements CompactionQueue {
    private static final Logger log = LoggerFactory.getLogger(CompactionQueueImpl.class);

    private final BlockingQueue<CompactionFile> filesToMove = new ArrayBlockingQueue<>(1048576);

    @Override
    public boolean offer(@NotNull CompactionFile file) {
        if (log.isTraceEnabled()) {
            log.trace("Putting file {} to the queue", file.getId());
        }
        checkNotNull(file, "File is required!");

        return filesToMove.offer(file);
    }

    @NotNull
    @Override
    public CompactionFile take() {
        try {
            return filesToMove.take();
        } catch (InterruptedException e) {
            log.error("Can't get file from the queue", e);
            throw new IllegalStateException(e);
        }
    }

    @Override
    public boolean isEmpty() {
        return filesToMove.isEmpty();
    }
}
