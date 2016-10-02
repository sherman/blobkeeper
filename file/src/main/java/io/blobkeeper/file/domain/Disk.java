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

import com.google.common.base.Objects;
import io.blobkeeper.index.domain.Partition;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.MoreObjects.toStringHelper;

public class Disk {
    private final int id;
    private final boolean writable;
    private final File writer;
    private final Partition activePartition;
    private final AtomicInteger errors = new AtomicInteger();

    public Disk(Builder builder) {
        this.id = builder.id;
        this.writable = builder.writable;
        this.writer = builder.writer;
        this.activePartition = builder.activePartition;
    }

    public int getId() {
        return id;
    }

    public boolean isWritable() {
        return writable;
    }

    public File getWriter() {
        return writer;
    }

    public void resetErrors() {
        this.errors.set(0);
    }

    public Partition getActivePartition() {
        return activePartition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Disk that = (Disk) o;

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
                .add("writable", writable)
                .add("writer", writer)
                .add("activePartition", activePartition)
                .add("errors", errors)
                .toString();
    }

    public AtomicInteger getErrors() {
        return errors;
    }

    public static class Builder {
        private final int id;
        private boolean writable;
        private File writer;
        private Partition activePartition;

        public Builder(int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }

        public Partition getActivePartition() {
            return activePartition;
        }

        public boolean isWritable() {
            return writable;
        }

        public Builder setWritable(boolean writable) {
            this.writable = writable;
            return this;
        }

        public Builder setWriter(@NotNull File writer) {
            this.writer = writer;
            return this;
        }

        public Builder setActivePartition(@NotNull Partition activePartition) {
            this.activePartition = activePartition;
            return this;
        }

        public Disk build() {
            // TODO: add checks
            return new Disk(this);
        }
    }


}
