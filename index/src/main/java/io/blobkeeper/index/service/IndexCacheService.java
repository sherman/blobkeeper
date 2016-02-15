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

import com.google.inject.ImplementedBy;
import io.blobkeeper.index.domain.CacheKey;
import io.blobkeeper.index.domain.IndexElt;
import org.jetbrains.annotations.NotNull;

@ImplementedBy(IndexCacheServiceImpl.class)
public interface IndexCacheService {
    IndexElt getById(@NotNull CacheKey key);

    void set(@NotNull IndexElt elt);

    void remove(@NotNull CacheKey key);

    void clear();
}
