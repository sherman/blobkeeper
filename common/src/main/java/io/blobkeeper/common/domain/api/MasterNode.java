package io.blobkeeper.common.domain.api;

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
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import static com.google.common.base.Objects.equal;

public class MasterNode extends BaseApiRequest {
    @JsonProperty
    private String address;

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (null == object) {
            return false;
        }

        if (!(object instanceof MasterNode)) {
            return false;
        }

        MasterNode o = (MasterNode) object;

        return equal(address, o.address) && equal(getToken(), o.getToken());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(address, getToken());
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .addValue(address)
                .addValue(getToken())
                .toString();
    }
}
