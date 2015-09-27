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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.blobkeeper.common.domain.Error;
import io.blobkeeper.common.domain.Result;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ResultValue extends ReturnValue<Result> {

    public ResultValue(Result value) {
        super(value);
    }

    public ResultValue(Error error) {
        super(error);
    }

    @JsonCreator
    public static ResultValue create(
            @JsonProperty("result") Result value,
            @JsonProperty("error") Error error
    ) {
        if (value != null) {
            return new ResultValue(value);
        } else {
            return new ResultValue(error);
        }
    }
}

