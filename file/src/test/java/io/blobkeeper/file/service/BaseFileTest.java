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

import io.blobkeeper.file.configuration.FileConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.testng.annotations.BeforeMethod;

import javax.inject.Inject;
import java.io.FileFilter;
import java.io.IOException;

import static io.blobkeeper.file.util.FileUtils.getDiskPathByDisk;

public abstract class BaseFileTest {

    @Inject
    protected FileConfiguration configuration;

    @Inject
    private FileListService fileListService;

    @BeforeMethod
    protected void deleteFiles() throws IOException {
        java.io.File basePath = new java.io.File(configuration.getBasePath());
        FileUtils.deleteDirectory(basePath);
        basePath.mkdir();

        java.io.File disk1 = getDiskPathByDisk(configuration, 0);
        if (!disk1.exists()) {
            disk1.mkdir();
        }

        java.io.File disk2 = getDiskPathByDisk(configuration, 1);
        if (!disk2.exists()) {
            disk2.mkdir();
        }

        for (int disk : fileListService.getDisks()) {
            java.io.File diskPath = getDiskPathByDisk(configuration, disk);
            for (java.io.File indexFile : diskPath.listFiles((FileFilter) new SuffixFileFilter(".data"))) {
                indexFile.delete();
            }
        }

        java.io.File uploadPath = new java.io.File(configuration.getUploadPath());
        if (!uploadPath.exists()) {
            uploadPath.mkdir();
        }
    }
}
