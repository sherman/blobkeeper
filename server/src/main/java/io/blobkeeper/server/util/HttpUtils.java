package io.blobkeeper.server.util;

import io.blobkeeper.file.domain.StorageFile;
import io.netty.handler.codec.http.multipart.FileUpload;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.regex.Pattern.compile;

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

public class HttpUtils {
    public static final int NOT_FOUND = -1;

    private static final Pattern idPattern = compile("/([0-9]{17,19})");
    private static final Pattern typePattern = compile("/([0-9]{17,19})/([0-9]{1,3})");
    private static final Pattern apiTokenPattern = compile("token=([0-9A-Za-z]{40})");

    private HttpUtils() {
    }

    public static long getId(@NotNull String path) {
        Matcher matcher = idPattern.matcher(path);
        if (!matcher.find()) {
            return NOT_FOUND;
        }

        return Long.parseLong(matcher.group(1));
    }

    public static int getType(@NotNull String path) {
        Matcher matcher = typePattern.matcher(path);
        if (!matcher.find()) {
            return NOT_FOUND;
        }

        return Integer.parseInt(matcher.group(2));
    }

    public static String getApiToken(@NotNull String path) {
        Matcher matcher = apiTokenPattern.matcher(path);
        if (!matcher.find()) {
            return null;
        }

        return matcher.group(1);
    }

    public static StorageFile.StorageFileBuilder buildStorageFile(@NotNull FileUpload file) {
        StorageFile.StorageFileBuilder builder = new StorageFile.StorageFileBuilder()
                .name(file.getName());

        try {
            if (file.isInMemory()) {
                byte[] data = file.getByteBuf().nioBuffer().array();
                return builder.data(data);
            } else {
                // copy to another file
                File copy = File.createTempFile("upload", file.getFilename());
                file.getFile().renameTo(copy);

                return builder.file(copy);
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
