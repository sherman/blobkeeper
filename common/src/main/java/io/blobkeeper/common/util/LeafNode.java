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

import com.google.common.collect.Range;

import java.io.Serializable;

import static java.lang.String.format;

public class LeafNode extends HashableNode implements Serializable {
    private static final long serialVersionUID = -7953752112831556664L;

    private Range<Long> range;

    public LeafNode(Range<Long> range) {
        this.range = range;
    }

    public Range<Long> getRange() {
        return range;
    }

    @Override
    public String toString() {
        return format("#<Leaf %s - %s %s>", range.lowerEndpoint(), range.upperEndpoint(), super.toString());
    }
}
