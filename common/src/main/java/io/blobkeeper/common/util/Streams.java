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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.of;
import static java.util.concurrent.CompletableFuture.allOf;

public class Streams {
    private static final Logger log = LoggerFactory.getLogger(Streams.class);

    private static final ExecutorService parallelExecutor = Executors.newCachedThreadPool(
            new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("ParallelizeWorker-%d")
                    .build()
    );

    private Streams() {
    }

    @NotNull
    public static <S, T> Stream<ResultWrapper<T>> parallelize(
            @NotNull Collection<S> source,
            @NotNull Function<S, Supplier<T>> mapper
    ) {
        // operations will be executed in parallel
        List<CompletableFuture<ResultWrapper<T>>> results = source.stream()
                .map(s -> CompletableFuture.supplyAsync(new ResultSupplier<>(mapper.apply(s)), parallelExecutor))
                .collect(ImmutableList.toImmutableList());

        return collect(results);
    }

    @NotNull
    public static <T> Stream<ResultWrapper<T>> parallelize(int n, @NotNull Supplier<T> supplier) {
        // operations will be executed in parallel
        List<CompletableFuture<ResultWrapper<T>>> results = IntStream.iterate(0, i -> i + 1)
                .limit(n)
                .boxed()
                .map(ignored -> CompletableFuture.supplyAsync(new ResultSupplier<>(supplier), parallelExecutor))
                .collect(ImmutableList.toImmutableList());

        return collect(results);
    }

    private static <T> Stream<ResultWrapper<T>> collect(List<CompletableFuture<ResultWrapper<T>>> results) {
        // wait for all operations
        return allOf(Iterables.toArray(results, CompletableFuture.class))
                // TODO: use apply async?
                .thenApply(ignored -> results.stream()
                        .map(CompletableFuture::join)
                        .collect(ImmutableList.toImmutableList())
                ).exceptionally(throwable -> {
                    log.error("Can't execute task", throwable);
                    return of();
                })
                .join().stream();
    }

    private static class ResultSupplier<T> implements Supplier<ResultWrapper<T>> {
        private final java.util.function.Supplier<T> inner;

        private ResultSupplier(java.util.function.Supplier<T> inner) {
            this.inner = inner;
        }

        @Override
        public ResultWrapper<T> get() {
            try {
                return new ResultWrapper<>(inner.get());
            } catch (Exception e) {
                log.error("Can't execute an action", e);
                return new ResultWrapper<>(e);
            }
        }
    }
}
