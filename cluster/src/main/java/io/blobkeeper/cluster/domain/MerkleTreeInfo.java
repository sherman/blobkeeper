package io.blobkeeper.cluster.domain;

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
import io.blobkeeper.common.util.MerkleTree;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;

public class MerkleTreeInfo implements Serializable {
    private static final long serialVersionUID = -1745989841836694570L;

    private int disk;
    private int partition;
    private MerkleTree tree;

    public int getDisk() {
        return disk;
    }

    public void setDisk(int disk) {
        this.disk = disk;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    @NotNull
    public MerkleTree getTree() {
        return tree;
    }

    public void setTree(MerkleTree tree) {
        this.tree = tree;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MerkleTreeInfo that = (MerkleTreeInfo) o;

        return Objects.equal(this.disk, that.disk) &&
                Objects.equal(this.partition, that.partition);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(disk, partition);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("disk", disk)
                .add("partition", partition)
                .add("tree", tree)
                .toString();
    }
}
