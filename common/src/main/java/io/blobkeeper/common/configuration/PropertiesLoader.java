package io.blobkeeper.common.configuration;

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

import com.google.common.base.Strings;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;

public class PropertiesLoader {
    private static final Logger log = LoggerFactory.getLogger(PropertiesLoader.class);

    @NotNull
    public static Properties loadProperties(String propertyFileName, Class<?> clazz) throws Exception {
        String fileName = System.getProperty(propertyFileName);

        Properties properties = new Properties();
        try {
            InputStream inputStream;
            if (!isNullOrEmpty(fileName)) {
                log.debug("Loading {} for {} from file: {}", propertyFileName, clazz.getSimpleName(), fileName);

                inputStream = new FileInputStream(fileName);
            } else {
                log.debug("Loading {} for {} from resource: {}", propertyFileName, clazz.getSimpleName(), fileName);

                ClassLoader loader = clazz.getClassLoader();
                URL url = loader.getResource(propertyFileName);
                assert null != url;
                inputStream = url.openStream();
            }
            properties.load(inputStream);
            return properties;
        } catch (IOException e) {
            throw new IllegalArgumentException(format("Failed to load config: %s", propertyFileName), e);
        }
    }
}
