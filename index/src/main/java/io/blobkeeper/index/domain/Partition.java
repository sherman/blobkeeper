package io.blobkeeper.index.domain;

import com.google.common.base.Objects;
import io.blobkeeper.common.util.MerkleTree;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Objects.equal;

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

public class Partition implements Serializable {
    private static final long serialVersionUID = -7366482353365325023L;

    private int id;
    private final AtomicLong offset = new AtomicLong();
    private long crc;
    private int disk;
    private MerkleTree tree;

    public Partition(int disk, int id) {
        this.disk = disk;
        this.id = id;
    }

    public long getOffset() {
        return offset.get();
    }

    public void setOffset(long offset) {
        this.offset.set(offset);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public long incrementOffset(long value) {
        return this.offset.addAndGet(value);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (null == object) {
            return false;
        }


        if (!(object instanceof Partition)) {
            return false;
        }

        Partition o = (Partition) object;

        return equal(disk, o.disk)
                && equal(id, o.id);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(disk, id);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .addValue(disk)
                .addValue(id)
                .addValue(offset.get())
                .addValue(crc)
                .toString();
    }

    public long getCrc() {
        return crc;
    }

    public void setCrc(long crc) {
        this.crc = crc;
    }

    public int getDisk() {
        return disk;
    }

    public void setDisk(int disk) {
        this.disk = disk;
    }

    public MerkleTree getTree() {
        return tree;
    }

    public void setTree(MerkleTree tree) {
        this.tree = tree;
    }
}
