package io.blobkeeper.common.util;

/*
 * Copyright (C) 2015-2016 by Denis M. Gabaydulin
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
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Longs;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;

public class Block {
    private final long id;
    private final List<BlockElt> blockElts;
    private long length;

    public Block(long id, List<BlockElt> blockElts) {
        this.id = id;
        this.blockElts = blockElts;

        long len = 0;
        for (BlockElt elt : blockElts) {
            len += elt.getLength();
        }
        this.length = len;
    }

    public long getId() {
        return id;
    }

    @NotNull
    @TestOnly
    public List<BlockElt> getBlockElts() {
        return ImmutableList.copyOf(blockElts);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Block that = (Block) o;

        return Objects.equal(this.id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("id", id)
                .add("elts", blockElts)
                .toString();
    }

    public byte[] toByteArray() {
        byte[] data = new byte[8 + (4 * blockElts.size())];

        long idValue = id;
        for (int i = 7; i >= 0; i--) {
            data[i] = (byte) (idValue & 0xffL);
            idValue >>= 8;
        }

        int k = 0;
        for (int i = 8; i < 4 * blockElts.size() + 8; i = i + 4) {
            int typeValue = blockElts.get(k++).getType();

            data[i] = (byte) (typeValue >> 24);
            data[i + 1] = (byte) (typeValue >> 16);
            data[i + 2] = (byte) (typeValue >> 8);
            data[i + 3] = (byte) (typeValue);
        }

        return data;
    }

    public long getLength() {
        return length;
    }
}
