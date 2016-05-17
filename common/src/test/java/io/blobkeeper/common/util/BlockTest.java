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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

public class BlockTest {
    private static final Logger log = LoggerFactory.getLogger(BlockTest.class);

    @Test
    public void toByteArray() {
        Block block = new Block(
                303274580351389722L,
                ImmutableList.of(
                        new BlockElt(303274580351389722L, 0, 0, 128, 128L),
                        new BlockElt(303274580351389722L, 1, 128, 128, 128L)
                )
        );

        assertEquals(block.toByteArray(), new byte[]{4, 53, 114, -97, -65, 64, 16, 26, 0, 0, 0, 0, 0, 0, 0, 1});
    }
}
