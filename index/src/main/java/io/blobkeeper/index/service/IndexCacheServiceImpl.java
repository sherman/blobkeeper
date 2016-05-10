package io.blobkeeper.index.service;

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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.blobkeeper.index.domain.CacheKey;
import io.blobkeeper.index.domain.IndexElt;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;

@Singleton
public class IndexCacheServiceImpl implements IndexCacheService {
    private static final Logger log = LoggerFactory.getLogger(IndexCacheServiceImpl.class);

    private final Cache<CacheKey, IndexElt> cache = CacheBuilder.newBuilder()
            // TODO: move to config
            .maximumSize(1048576)
            .build();

    @Override
    public IndexElt getById(@NotNull CacheKey key) {
        return cache.getIfPresent(key);
    }

    @Override
    public void set(@NotNull IndexElt elt) {
        cache.put(new CacheKey(elt.getId(), elt.getType()), elt);
    }

    @Override
    public void remove(@NotNull CacheKey key) {
        cache.invalidate(key);
    }

    @Override
    public void clear() {
        cache.cleanUp();
    }
}
