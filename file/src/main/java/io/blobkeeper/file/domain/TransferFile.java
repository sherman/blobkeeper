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
import io.blobkeeper.index.domain.DiskIndexElt;

import java.io.Serializable;

public class TransferFile implements Serializable {
    private static final long serialVersionUID = -4623936738388070578L;

    private final DiskIndexElt from;
    private final DiskIndexElt to;

    public TransferFile(DiskIndexElt from, DiskIndexElt to) {
        this.from = from;
        this.to = to;
    }

    public DiskIndexElt getFrom() {
        return from;
    }

    public DiskIndexElt getTo() {
        return to;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TransferFile that = (TransferFile) o;

        return Objects.equal(this.from, that.from) &&
                Objects.equal(this.to, that.to);
    }


    @Override
    public int hashCode() {
        return Objects.hashCode(from, to);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("from", from)
                .add("to", to)
                .toString();
    }
}
