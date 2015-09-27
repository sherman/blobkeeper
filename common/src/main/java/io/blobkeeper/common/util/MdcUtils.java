package io.blobkeeper.common.util;

import io.blobkeeper.common.logging.MdcContext;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.MDC;

import java.util.HashMap;
import java.util.Map;

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

public class MdcUtils {
    private static final Logger log = getLogger(MdcUtils.class);

    private MdcUtils() {
    }

    public static void setCurrentContext(@NotNull MdcContext context) {
        MDC.setContextMap(context.getContext());
    }

    public static void putToCurrentContext(@NotNull String key, @NotNull String value) {
        MDC.put(key, value);
    }

    public static void removeFromCurrentContext(@NotNull String key) {
        MDC.remove(key);
    }

    public static void clearCurrentContext() {
        MDC.clear();
    }

    @SuppressWarnings("unchecked")
    @NotNull
    public static MdcContext getCurrentContext() {
        Map<String, String> context = MDC.getCopyOfContextMap();
        if (null == context) {
            log.warn("Can't find current context");
            context = new HashMap<>();
        }

        return new MdcContext(context);
    }
}
