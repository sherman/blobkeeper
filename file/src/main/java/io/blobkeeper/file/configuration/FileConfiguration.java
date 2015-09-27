package io.blobkeeper.file.configuration;

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

import org.jetbrains.annotations.TestOnly;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

@Singleton
public class FileConfiguration {

    @Inject
    @Named("blobkeeper.base.path")
    private String basePath;

    @Inject
    @Named("blobkeeper.file.max.size")
    private long maxFileSize;

    @Inject
    @Named("blobkeeper.disk.max.errors")
    private int maxDiskWriteErrors;

    public String getBasePath() {
        return basePath;
    }

    public long getMaxFileSize() {
        return maxFileSize;
    }

    @TestOnly
    public void setMaxFileSize(long maxFileSize) {
        this.maxFileSize = maxFileSize;
    }

    public int getMaxDiskWriteErrors() {
        return maxDiskWriteErrors;
    }
}
