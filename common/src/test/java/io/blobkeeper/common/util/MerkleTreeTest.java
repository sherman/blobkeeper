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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Range;
import io.blobkeeper.common.service.IdGeneratorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.*;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class MerkleTreeTest {
    private static final Logger log = LoggerFactory.getLogger(MerkleTreeTest.class);

    private Random random = new Random();

    @Test
    public void buildTreeEmpty() {
        MerkleTree tree = Utils.createEmptyTree(Range.openClosed(1L, 100L), 3);

        LeafNode node1 = new LeafNode(Range.openClosed(1L, 50L));
        node1.addHash(HashableNode.EMPTY_HASH, 0);
        LeafNode node2 = new LeafNode(Range.openClosed(50L, 100L));
        node2.addHash(HashableNode.EMPTY_HASH, 0);

        assertEquals(tree.getLeafNodes(), ImmutableList.of(node1, node2));
        assertNotNull(tree.getRoot().getHash());
    }

    @Test
    public void buildTree() {
        MerkleTree tree = Utils.createTree(Range.openClosed(1L, 100L), 3, ImmutableSortedMap.of(42L, new Block(1, 2, 3, 4)));
        tree.getLeafNodes();

        LeafNode node1 = new LeafNode(Range.openClosed(1L, 50L));
        node1.addHash(new byte[]{0, 0, 0, 0, 0, 0, 0, 1}, 1);
        LeafNode node2 = new LeafNode(Range.openClosed(50L, 100L));
        node2.addHash(HashableNode.EMPTY_HASH, 0);

        assertEquals(tree.getLeafNodes(), ImmutableList.of(node1, node2));
        assertNotNull(tree.getRoot().getHash());
    }

    @Test
    public void difference() throws InterruptedException {
        IdGeneratorService service = new IdGeneratorService();

        List<Block> originalBlocks = new ArrayList<>();

        long min = 0, max = 0;

        int offset = 0;
        for (int i = 0; i < 10000; i++) {
            long id = service.generate(1);
            int length = random.nextInt(65536);
            originalBlocks.add(new Block(id, offset, length, 42));
            offset += length;

            if (min == 0) {
                min = id;
            }
            max = id;

            Thread.sleep(1);
        }

        SortedMap<Long, Block> blocks = originalBlocks.stream()
                .collect(toMap(Block::getId, Function.<Block>identity(), Utils.throwingMerger(), TreeMap::new));

        SortedMap<Long, Block> blocks2 = originalBlocks.stream()
                .limit(9500)
                .collect(toMap(Block::getId, Function.<Block>identity(), Utils.throwingMerger(), TreeMap::new));

        Range<Long> ranges = Range.openClosed(min - 1, max);

        MerkleTree tree = new MerkleTree(ranges, 64);
        MerkleTree.fillTree(tree, blocks);

        MerkleTree tree2 = new MerkleTree(ranges, 64);
        MerkleTree.fillTree(tree2, blocks2);

        assertEquals(
                MerkleTree.difference(tree, tree2)
                        .stream()
                        .count(),
                4);
    }
}
