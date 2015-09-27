package io.blobkeeper.common.domain;

import com.google.common.base.Objects;
import org.jetbrains.annotations.NotNull;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Objects.equal;

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

public class Error {
    private final ErrorCode code;
    private final String message;
    private final String originalMessage;

    public Error(ErrorCode code, String message) {
        this.code = code;
        this.message = message;
        this.originalMessage = null;
    }

    public Error(ErrorCode code, String message, String originalMessage) {
        this.code = code;
        this.message = message;
        this.originalMessage = originalMessage;
    }

    public ErrorCode getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (null == object) {
            return false;
        }

        if (!(object instanceof Error)) {
            return false;
        }

        Error o = (Error) object;

        return equal(code, o.code)
                && equal(message, o.message)
                && equal(originalMessage, o.originalMessage);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(code, message, originalMessage);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .addValue(code)
                .addValue(message)
                .addValue(originalMessage)
                .toString();
    }

    public static Error createError(@NotNull ErrorCode errorCode, @NotNull String errorMessage) {
        return new Error(errorCode, errorMessage);
    }
}
