package io.blobkeeper.file.domain;

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

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

public class CompactionFile {
    private long id;
    private int type;

    public CompactionFile(long id, int type) {
        this.id = id;
        this.type = type;
    }

    public long getId() {
        return id;
    }

    public int getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CompactionFile that = (CompactionFile) o;

        return Objects.equal(this.id, that.id)
                && Objects.equal(this.type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id, type);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", id)
                .add("type", type)
                .toString();
    }
}
