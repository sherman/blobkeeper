package io.blobkeeper.index.domain;

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
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.Map;

import static com.google.common.base.Objects.equal;

public class IndexTempElt implements Serializable {
    private static final long serialVersionUID = -6885487468590312309L;

    private final long id;
    private final int type;
    private final long created;
    private final Map<String, Object> metadata;
    private final String file;

    public IndexTempElt(IndexTempEltBuilder builder) {
        this.id = builder.id;
        this.type = builder.type;
        this.created = builder.created;
        this.metadata = builder.metadata;
        this.file = builder.file;
    }

    public long getId() {
        return id;
    }

    public int getType() {
        return type;
    }

    public long getCreated() {
        return created;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public String getFile() {
        return file;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (null == object) {
            return false;
        }


        if (!(object instanceof IndexTempElt)) {
            return false;
        }

        IndexTempElt o = (IndexTempElt) object;

        return equal(id, o.id)
                && equal(type, o.type);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id, type);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .addValue(id)
                .addValue(type)
                .addValue(metadata)
                .addValue(created)
                .addValue(file)
                .toString();
    }

    public static class IndexTempEltBuilder {
        private long id;
        private int type;
        private long created;
        private Map<String, Object> metadata;
        private String file;

        public IndexTempEltBuilder id(long id) {
            this.id = id;
            return this;
        }

        public IndexTempEltBuilder type(int type) {
            this.type = type;
            return this;
        }

        public IndexTempEltBuilder created(long created) {
            this.created = created;
            return this;
        }

        public IndexTempEltBuilder file(@NotNull String file) {
            this.file = file;
            return this;
        }

        public IndexTempEltBuilder metadata(@NotNull Map<String, Object> metadata) {
            this.metadata = metadata;
            return this;
        }

        public IndexTempElt build() {
            return new IndexTempElt(this);
        }
    }
}
