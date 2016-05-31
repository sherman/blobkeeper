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
 */

import com.google.inject.Injector;
import io.blobkeeper.common.service.BaseMultipleInjectorTest;
import io.blobkeeper.file.configuration.FileConfiguration;
import io.blobkeeper.file.util.FileUtils;
import io.blobkeeper.index.service.IndexService;
import org.apache.commons.io.filefilter.SuffixFileFilter;

import java.io.FileFilter;
import java.io.IOException;

import static org.apache.commons.io.FileUtils.deleteDirectory;

public abstract class BaseMultipleInjectorFileTest extends BaseMultipleInjectorTest {

    protected void removeDisk(Injector injector, int disk) throws IOException {
        FileConfiguration configuration = injector.getInstance(FileConfiguration.class);
        java.io.File diskPath = FileUtils.getDiskPathByDisk(configuration, disk);
        deleteDirectory(diskPath);
    }

    protected void addDisk(Injector injector, int disk) {
        FileConfiguration configuration = injector.getInstance(FileConfiguration.class);
        java.io.File diskPath = FileUtils.getDiskPathByDisk(configuration, disk);
        diskPath.mkdir();
    }

    protected void prepareDataDirectory(Injector injector) throws IOException {
        FileConfiguration configuration = injector.getInstance(FileConfiguration.class);
        java.io.File basePath = new java.io.File(configuration.getBasePath());
        deleteDirectory(basePath);
        basePath.mkdir();

        addDisk(injector, 0);
        addDisk(injector, 1);

        for (int disk : injector.getInstance(FileListService.class).getDisks()) {
            java.io.File diskPath = FileUtils.getDiskPathByDisk(configuration, disk);
            for (java.io.File indexFile : diskPath.listFiles((FileFilter) new SuffixFileFilter(".data"))) {
                indexFile.delete();
            }
        }

        java.io.File uploadPath = new java.io.File(configuration.getUploadPath());
        if (!uploadPath.exists()) {
            uploadPath.mkdir();
        }
    }

    protected void clearIndex() {
        firstServerInjector.getInstance(IndexService.class).clear();
    }
}
