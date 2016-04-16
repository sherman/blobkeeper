package io.blobkeeper.index.domain;

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

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Objects.equal;
import static org.joda.time.DateTime.now;
import static org.joda.time.DateTimeZone.UTC;

public class IndexElt implements Serializable {
    private static final long serialVersionUID = -6835974446240794106L;

    public static final IndexElt EMPTY = new IndexEltBuilder()
            .id(0L)
            .build();

    public static final int DEFAULT_TYPE = 0;

    public static final String HEADERS = "headers";
    public static final String NAME = "name";
    public static final String AUTH_TOKENS = "authTokens";

    private final long id;
    private final int type;
    private final long created;
    private final long updated;
    private final boolean deleted;
    private final long crc;
    private final Map<String, Object> metadata;
    private final DiskIndexElt diskIndexElt;

    public IndexElt(IndexEltBuilder builder) {
        this.id = builder.id;
        this.type = builder.type;
        this.created = builder.created;
        this.updated = builder.updated;
        this.deleted = builder.deleted;
        this.crc = builder.crc;
        this.metadata = builder.metadata;
        this.diskIndexElt = new DiskIndexElt(builder.partition, builder.offset, builder.length);
    }

    public long getId() {
        return id;
    }

    public long getOffset() {
        return diskIndexElt.getOffset();
    }

    public long getLength() {
        return diskIndexElt.getLength();
    }

    public long getCreated() {
        return created;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public int getType() {
        return type;
    }

    public Partition getPartition() {
        return diskIndexElt.getPartition();
    }

    public DiskIndexElt getDiskIndexElt() {
        return diskIndexElt;
    }

    public long getUpdated() {
        return updated;
    }

    @NotNull
    @SuppressWarnings("unchecked")
    public Multimap<String, String> getHeaders() {
        Multimap<String, String> headers = (Multimap<String, String>) metadata.get(HEADERS);
        if (null != headers) {
            return headers;
        } else {
            return ImmutableMultimap.of();
        }
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (null == object) {
            return false;
        }


        if (!(object instanceof IndexElt)) {
            return false;
        }

        IndexElt o = (IndexElt) object;

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
                .addValue(diskIndexElt)
                .addValue(id)
                .addValue(type)
                .addValue(metadata)
                .addValue(created)
                .addValue(updated)
                .addValue(deleted)
                .toString();
    }

    public long getCrc() {
        return crc;
    }

    public boolean isAuthRequired() {
        List<String> authTokens = getAuthTokens();
        return null != authTokens && !authTokens.isEmpty();
    }

    public boolean isAllowed(@NotNull String authToken) {
        List<String> authTokens = getAuthTokens();
        return authTokens.contains(authToken);
    }

    @NotNull
    public CacheKey toCacheKey() {
        return new CacheKey(id, type);
    }

    private List<String> getAuthTokens() {
            return (List<String>) getMetadata().get(IndexElt.AUTH_TOKENS);
        }

    public static class IndexEltBuilder {
        private long id;
        private int type;
        private Partition partition;
        private long created = now(UTC).getMillis();
        private long updated = now(UTC).getMillis();
        private boolean deleted = false;
        private long offset;
        private long length;
        private long crc;
        private Map<String, Object> metadata = new HashMap<>();

        public IndexEltBuilder id(long id) {
            this.id = id;
            return this;
        }

        public IndexEltBuilder type(int type) {
            this.type = type;
            return this;
        }

        public IndexEltBuilder partition(@NotNull Partition partition) {
            this.partition = partition;
            return this;
        }

        public IndexEltBuilder created(long created) {
            this.created = created;
            return this;
        }

        public IndexEltBuilder updated(long updated) {
            this.updated = updated;
            return this;
        }

        public IndexEltBuilder deleted(boolean deleted) {
            this.deleted = deleted;
            return this;
        }

        public IndexEltBuilder offset(long offset) {
            this.offset = offset;
            return this;
        }

        public IndexEltBuilder length(long length) {
            this.length = length;
            return this;
        }

        public IndexEltBuilder crc(long crc) {
            this.crc = crc;
            return this;
        }

        public IndexEltBuilder metadata(@NotNull Map<String, Object> metadata) {
            this.metadata = metadata;
            return this;
        }

        public IndexElt build() {
            return new IndexElt(this);
        }
    }
}
