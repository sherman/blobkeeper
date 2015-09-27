package io.blobkeeper.index.util;

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

import com.google.common.collect.ComparisonChain;
import io.blobkeeper.index.domain.IndexElt;

import java.util.Comparator;
import java.util.function.Consumer;

public class MinMaxConsumer implements Consumer<IndexElt> {
    private IndexElt min = IndexElt.EMPTY;
    private IndexElt max = IndexElt.EMPTY;

    private static final IndexEltComparator comparator = new IndexEltComparator();

    public IndexElt getMin() {
        return min;
    }

    public IndexElt getMax() {
        return max;
    }

    @Override
    public void accept(IndexElt indexElt) {
        if (comparator.compare(min, indexElt) < 0) {
            min = indexElt;
        }

        if (comparator.compare(max, indexElt) < 0) {
            max = indexElt;
        }
    }

    public boolean isEmpty() {
        return min.equals(IndexElt.EMPTY) && max.equals(IndexElt.EMPTY);
    }

    public void combine(MinMaxConsumer consumer) {
            /*_*/
    }

    private static class IndexEltComparator implements Comparator<IndexElt> {

        @Override
        public int compare(IndexElt o1, IndexElt o2) {
            return ComparisonChain
                    .start()
                    .compare(o1.getId(), o2.getId())
                    .result();
        }
    }
}


