package io.blobkeeper.common.util;

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
import java.util.Arrays;

import static com.google.common.base.Objects.equal;
import static io.blobkeeper.common.util.Hex.bytesToHex;
import static io.blobkeeper.common.util.Utils.xor;

public abstract class HashableNode implements Serializable {
    private static final long serialVersionUID = 8262958047513342591L;

    public static final byte[] EMPTY_HASH = new byte[0];

    protected byte[] hash;
    protected long length; // in bytes
    protected int blocks; // num of blocks

    HashableNode() {
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HashableNode that = (HashableNode) o;

        return this.hash != null && that.hash != null && equal(bytesToHex(this.hash), bytesToHex(that.hash));
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(bytesToHex(hash));
    }

    @Override
    public String toString() {
        if (hash == null) {
            return "null";
        }

        return "[" + bytesToHex(hash) + "]";
    }

    public byte[] getHash() {
        return hash;
    }

    public long getLength() {
        return length;
    }

    public int getBlocks() {
        return blocks;
    }

    void addHash(byte[] hash, long length) {
        if (this.hash == null) {
            this.hash = hash;
        } else {
            this.hash = xor(this.hash, hash);
        }

        this.length += length;

        if (!Arrays.equals(hash, EMPTY_HASH)) {
            this.blocks += 1;
        }
    }

    HashableNode calculate() {
        return this;
    }
}
