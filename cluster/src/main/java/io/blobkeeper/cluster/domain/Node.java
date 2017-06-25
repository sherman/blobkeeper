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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.util.StdConverter;
import com.google.common.base.Objects;
import org.jetbrains.annotations.NotNull;
import org.jgroups.Address;

import java.io.Serializable;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Objects.equal;

public class Node implements Serializable {
    private static final long serialVersionUID = 5381696321695712471L;

    @JsonProperty
    private final Role role;

    @JsonProperty
    @JsonSerialize(converter = ToStringConverter.class)
    private final Address address;

    @JsonProperty
    private final long updated;

    public Node(Role role, Address address, long updated) {
        this.role = role;
        this.address = address;
        this.updated = updated;
    }

    @NotNull
    public Role getRole() {
        return role;
    }

    @NotNull
    public Address getAddress() {
        return address;
    }

    public long getUpdated() {
        return updated;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (null == object) {
            return false;
        }

        if (!(object instanceof Node)) {
            return false;
        }

        Node o = (Node) object;

        return equal(address, o.address);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(address);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .addValue(role)
                .addValue(address)
                .addValue(updated)
                .toString();
    }

    private static class ToStringConverter extends StdConverter<Address, String> {
        @Override
        public String convert(Address value) {
            return value.toString();
        }
    }
}
