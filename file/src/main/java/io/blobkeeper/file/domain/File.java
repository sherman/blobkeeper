package io.blobkeeper.file.domain;

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

import com.google.common.collect.ComparisonChain;
import io.blobkeeper.common.util.AlphaNumComparator;
import org.apache.commons.io.FilenameUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.Ordering.from;

public class File implements Comparable<File> {
    private static final Logger log = LoggerFactory.getLogger(File.class);
    private final RandomAccessFile accessFile;
    private final String name;
    private final FileChannel channel;

    public File(java.io.File absolutePath) {
        name = FilenameUtils.getName(absolutePath.getAbsolutePath());

        try {
            accessFile = new RandomAccessFile(absolutePath, "rw");
            accessFile.seek(0);
            this.channel = accessFile.getChannel();
        } catch (FileNotFoundException e) {
            log.error("Can't open file", e);
            throw new IllegalArgumentException(e);
        } catch (IOException e) {
            log.error("Can't seek file", e);
            throw new IllegalArgumentException(e);
        }
    }

    public void preallocate(long fileSize) {
        try {
            accessFile.setLength(fileSize);
        } catch (IOException e) {
            log.error("Can't preallocate file", e);
            throw new IllegalArgumentException(e);
        }
    }

    public void close() {
        try {
            accessFile.close();
        } catch (IOException e) {
            log.error("Can't close file", e);
            throw new IllegalArgumentException(e);
        }
    }

    public FileChannel getFileChannel() {
        return channel;
    }

    public String getName() {
        return name;
    }

    public long getLength() {
        try {
            return channel.size();
        } catch (IOException e) {
            log.error("Can't get length of the file", e);
            throw new IllegalStateException(e);
        }
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .addValue(name)
                .toString();
    }

    @Override
    public int compareTo(@NotNull File other) {
        return ComparisonChain
                .start()
                .compare(this.name, other.getName(), from(new AlphaNumComparator()))
                .result();
    }
}