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

import com.google.common.base.Objects;

import java.io.Serializable;

import static com.google.common.base.MoreObjects.toStringHelper;

public class DiskIndexElt implements Serializable {
    private static final long serialVersionUID = 3363780875936734274L;

    private final long offset;
    private final long length;
    private final Partition partition;

    public DiskIndexElt(Partition partition, long offset, long length) {
        this.offset = offset;
        this.length = length;
        this.partition = partition;
    }

    public long getOffset() {
        return offset;
    }

    public long getLength() {
        return length;
    }

    public Partition getPartition() {
        return partition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DiskIndexElt that = (DiskIndexElt) o;

        return Objects.equal(this.offset, that.offset) &&
                Objects.equal(this.length, that.length) &&
                Objects.equal(this.partition, that.partition);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(offset, length, partition);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("offset", offset)
                .add("length", length)
                .add("partition", partition)
                .toString();
    }
}
