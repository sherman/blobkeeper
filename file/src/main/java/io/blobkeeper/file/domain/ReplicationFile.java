package io.blobkeeper.file.domain;

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
import io.blobkeeper.index.domain.DiskIndexElt;

import java.io.Serializable;

import static com.google.common.base.MoreObjects.toStringHelper;

public class ReplicationFile implements Serializable {
    private static final long serialVersionUID = -2058060127155955252L;

    private final DiskIndexElt index;
    private final byte[] data;

    public ReplicationFile(DiskIndexElt index, byte[] data) {
        this.index = index;
        this.data = data;
    }

    public DiskIndexElt getIndex() {
        return index;
    }

    public byte[] getData() {
        return data;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ReplicationFile that = (ReplicationFile) o;

        return Objects.equal(this.index, that.index) &&
                Objects.equal(this.data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(index, data);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("index", index)
                .toString();
    }
}
