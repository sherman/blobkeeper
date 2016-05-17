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

import com.google.common.collect.BinaryTreeTraverser;
import com.google.common.collect.Range;
import com.google.common.primitives.Longs;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Range.openClosed;
import static io.blobkeeper.common.util.GuavaCollectors.toImmutableList;
import static io.blobkeeper.common.util.HashableNode.EMPTY_HASH;
import static io.blobkeeper.common.util.Utils.midPoint;
import static java.lang.Math.pow;

public class MerkleTree implements Serializable {
    private static final long serialVersionUID = 5208542351647956821L;
    private static final Logger log = LoggerFactory.getLogger(MerkleTree.class);

    public static final int MAX_LEVEL = 64;

    private int maxDepth;
    private Range<Long> fullRange;
    private int size;
    private HashableNode root;

    public MerkleTree(Range<Long> fullRange, int maxDepth) {
        checkArgument(!fullRange.isEmpty(), "Range is empty!");

        this.size = 0;
        this.fullRange = fullRange;
        this.maxDepth = maxDepth;
        init();
    }

    public void calculate() {
        root.calculate();
    }

    @Override
    public String toString() {
        return root.toString();
    }

    public List<LeafNode> getLeafNodes() {
        TreeTraverser treeTraverser = new TreeTraverser();
        return treeTraverser.breadthFirstTraversal(root)
                .toList()
                .stream()
                .filter(node -> node instanceof LeafNode)
                .map(node -> (LeafNode) node)
                .collect(toImmutableList());
    }

    /**
     * {@param blocks} must be sorted
     */
    public static void fillTree(@NotNull MerkleTree tree, @NotNull SortedMap<Long, Block> blocks) {
        List<LeafNode> nodes = tree.getLeafNodes();
        Iterator<LeafNode> iterator = nodes.iterator();

        LeafNode current = null;
        if (iterator.hasNext()) {
            current = iterator.next();
        }

        for (Long hash : blocks.keySet()) {
            Block block = blocks.get(hash);

            while (current != null && !current.getRange().contains(hash) && iterator.hasNext()) {
                current = iterator.next();

                if (!current.getRange().contains(hash)) {
                    current.addHash(EMPTY_HASH, 0);
                }
            }

            if (current != null) {
                // FIXME
                current.addHash(block.toByteArray(), block.getLength());
            } else {
                break;
            }
        }

        if (current != null && current.getHash() == null) {
            current.addHash(EMPTY_HASH, 0);
        }

        while (iterator.hasNext()) {
            LeafNode node = iterator.next();
            node.addHash(EMPTY_HASH, 0);
        }
    }

    public static List<LeafNode> difference(@NotNull MerkleTree one, @Nullable MerkleTree two) {
        checkNotNull(one, "Expected tree is required!");

        if (null == two) {
            return one.getLeafNodes();
        }

        Deque<HashableNode> nodes1 = new ArrayDeque<>();
        Deque<HashableNode> nodes2 = new ArrayDeque<>();

        HashableNode current1 = one.root;
        HashableNode current2 = two.root;

        List<LeafNode> diff = new ArrayList<>();

        while ((!nodes1.isEmpty() || current1 != null) && (!nodes2.isEmpty() || current2 != null)) {
            if (current1 != null && current2 != null && current1 instanceof LeafNode && current2 instanceof LeafNode && !current1.equals(current2)) {
                log.debug("Nodes are diff {} : {}", current1, current2);
                diff.add((LeafNode) current2);
            }

            if (current1 instanceof BranchNode) {
                HashableNode rightNode = ((BranchNode) current1).getRight();
                if (rightNode != null) {
                    nodes1.push(rightNode);
                }
                current1 = ((BranchNode) current1).getLeft();
            } else {
                if (!nodes1.isEmpty()) {
                    current1 = nodes1.pop();
                } else {
                    current1 = null;
                }
            }

            if (current2 instanceof BranchNode) {
                HashableNode rightNode = ((BranchNode) current2).getRight();
                if (rightNode != null) {
                    nodes2.push(rightNode);
                }
                current2 = ((BranchNode) current2).getLeft();
            } else {
                if (!nodes2.isEmpty()) {
                    current2 = nodes2.pop();
                } else {
                    current2 = null;
                }
            }
        }

        return diff;
    }

    private void init() {
        // determine the depth to which we can safely split the tree
        int depth = (int) (Math.log10(maxDepth) / Math.log10(2));
        root = _init(fullRange.lowerEndpoint(), fullRange.upperEndpoint(), 0, depth);
        size = (int) pow(2, depth);
    }


    private HashableNode _init(Long left, Long right, int depth, int max) {
        if (depth == max) {
            return new LeafNode(openClosed(left, right));
        }

        long midpoint = midPoint(left, right);

        log.trace("M: {}", midpoint);

        if (Objects.equals(left, midpoint) || Objects.equals(right, midpoint)) {
            return new LeafNode(openClosed(left, right));
        }

        ++depth;
        HashableNode leftNode = _init(left, midpoint, depth, max);
        HashableNode rightNode = _init(midpoint, right, depth, max);

        log.trace("B: {} - {}, {} - {}", left, midpoint, midpoint, right);

        return new BranchNode(midpoint, leftNode, rightNode);
    }

    public HashableNode getRoot() {
        return root;
    }

    private static class TreeTraverser extends BinaryTreeTraverser<HashableNode> {
        @Override
        public com.google.common.base.Optional<HashableNode> leftChild(HashableNode parent) {
            if (parent instanceof BranchNode) {
                return com.google.common.base.Optional.fromNullable(((BranchNode) parent).getLeft());
            } else {
                return com.google.common.base.Optional.absent();
            }
        }

        @Override
        public com.google.common.base.Optional<HashableNode> rightChild(HashableNode parent) {
            if (parent instanceof BranchNode) {
                return com.google.common.base.Optional.fromNullable(((BranchNode) parent).getRight());
            } else {
                return com.google.common.base.Optional.absent();
            }
        }
    }
}
