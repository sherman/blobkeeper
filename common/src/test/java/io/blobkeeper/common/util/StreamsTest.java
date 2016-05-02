package io.blobkeeper.common.util;

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

import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;

public class StreamsTest {
    @Test
    public void parallelize() {
        List<Long> delays = Lists.newArrayList(1001L, 1002L);

        assertEquals((long) Streams.parallelize(delays, delay -> () -> {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException ignored) {
            }

            return delay + delay;
        })
                .filter(r -> !r.hasError())
                .map(ResultWrapper::getResult)
                .reduce(0L, (a, b) -> a + b), 4006L);
    }

    @Test
    public void parallelizeWithException() {
        List<Long> delays = Lists.newArrayList(1001L, 1002L);

        assertEquals((long) Streams.parallelize(delays, delay -> () -> {
            if (delay == 1002L) {
                throw new RuntimeException();
            }

            try {
                Thread.sleep(delay);
            } catch (InterruptedException ignored) {
            }

            return delay + delay;
        })
                .filter(r -> !r.hasError())
                .map(ResultWrapper::getResult)
                .reduce(0L, (a, b) -> a + b), 2002L);
    }

    @Test
    public void parallelizeSupplier() {
        assertEquals((long) Streams.parallelize(3, () -> 42L)
                .filter(r -> !r.hasError())
                .map(ResultWrapper::getResult)
                .reduce(0L, (a, b) -> a + b), 126L);
    }
}
