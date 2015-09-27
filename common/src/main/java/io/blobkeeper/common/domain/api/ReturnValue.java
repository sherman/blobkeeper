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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import io.blobkeeper.common.domain.Error;

import static com.google.common.base.MoreObjects.toStringHelper;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ReturnValue<T> {
    protected final T result;
    protected final Error error;

    public ReturnValue(T value) {
        this.result = value;
        error = null;
    }

    public ReturnValue(Error error) {
        this.error = error;
        result = null;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Error getError() {
        return error;
    }

    public T getResult() {
        return result;
    }

    @JsonIgnore
    public boolean hasError() {
        return error != null;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (null == object) {
            return false;
        }

        @SuppressWarnings("unchecked")
        ReturnValue<T> o = (ReturnValue<T>) object;

        return
                Objects.equal(result, o.result)
                        && Objects.equal(error, o.error);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(result, error);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .addValue(result)
                .addValue(error)
                .toString();
    }

    @JsonCreator
    public static <T> ReturnValue<T> create(
            @JsonProperty("result") T value,
            @JsonProperty("error") Error error
    ) {
        if (null != value) {
            return new ReturnValue<>(value);
        } else {
            return new ReturnValue<>(error);
        }
    }
}

