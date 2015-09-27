package io.blobkeeper.common.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

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

public class IdGeneratorServiceTest {
    private static final Logger log = LoggerFactory.getLogger(IdGeneratorServiceTest.class);

    @Test
    public void generateId() {
        IdGeneratorService idGeneratorService = new IdGeneratorService();
        log.info("id {}", idGeneratorService.generate(1));
    }

    @Test
    public void getShard() {
        IdGeneratorService idGeneratorService = new IdGeneratorService();
        for (int i = 0; i < 1024; i++) {
            long id = idGeneratorService.generate(i);
            assertEquals(idGeneratorService.getShard(id), i);
        }
    }

    @Test
    public void getTimestamp() {
        long mills = currentTimeMillis();
        IdGeneratorService idGeneratorService = new IdGeneratorService();
        long id = idGeneratorService.generate(1);
        assertEquals(idGeneratorService.getTimestamp(id), mills);
    }

    @Test
    public void generateSingleThread() {
        Set<Long> ids = new HashSet<>();
        IdGeneratorService idGeneratorService = new IdGeneratorService();
        for (int i = 0; i < 4096; i++) {
            long id = idGeneratorService.generate(1);
            assertTrue(ids.add(id));
        }
    }

    @Test
    public void multiThread() throws InterruptedException {
        int threads = 64;
        final int idsNum = 10000;

        final IdGeneratorService service = new IdGeneratorService();
        ExecutorService executorService = newFixedThreadPool(threads * 2);

        final CountDownLatch latch = new CountDownLatch(threads);

        final ConcurrentHashMap<Long, Boolean> ids = new ConcurrentHashMap<>();

        for (int i = 0; i < threads; i++) {
            executorService.submit(
                    new Runnable() {
                        @Override
                        public void run() {
                            try {
                                for (int j = 0; j < idsNum; j++) {
                                    long id = service.generate(1);
                                    if (null != ids.putIfAbsent(id, true)) {
                                        throw new RuntimeException("" + id);
                                    }
                                }
                            } catch (Exception e) {
                                log.error("Exception", e);
                            } finally {
                                latch.countDown();
                            }

                        }
                    }
            );
        }

        latch.await();

        assertEquals(ids.size(), threads * idsNum);
    }

    @Test
    public void getHash() {
        IdGeneratorService service = new IdGeneratorService();
        assertEquals(service.getHash(187525286248583168L).asLong(), -501962254452770615L);
    }
}
