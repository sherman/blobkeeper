package io.blobkeeper.index.domain;

import com.google.common.base.Objects;

import java.io.Serializable;

import static com.google.common.base.Objects.equal;

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

public class CacheKey implements Serializable {
    private static final long serialVersionUID = -2385302128000045986L;

    private final long id;
    private final int typeId;

    public CacheKey(long id, int typeId) {
        this.id = id;
        this.typeId = typeId;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (null == object) {
            return false;
        }


        if (!(object instanceof CacheKey)) {
            return false;
        }

        CacheKey o = (CacheKey) object;

        return equal(id, o.id)
                && equal(typeId, o.typeId);
    }

    /**
     * id are globally unique by shard
     */
    @Override
    public int hashCode() {
        return Objects.hashCode(id, typeId);
    }

    public long getId() {
        return id;
    }

    public int getTypeId() {
        return typeId;
    }
}
