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

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Range;
import org.jetbrains.annotations.NotNull;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.SortedMap;
import java.util.function.BinaryOperator;

public class Utils {
    private Utils() {
    }

    /**
     * @return The bitwise XOR of the inputs. The output will be the same length as the
     * longer input, but if either input is null, the output will be null.
     */
    public static byte[] xor(byte[] left, byte[] right) {
        if (left == null || right == null)
            return null;
        if (left.length > right.length) {
            byte[] swap = left;
            left = right;
            right = swap;
        }

        // left.length is now <= right.length
        byte[] out = Arrays.copyOf(right, right.length);
        for (int i = 0; i < left.length; i++) {
            out[i] = (byte) ((left[i] & 0xFF) ^ (right[i] & 0xFF));
        }
        return out;
    }

    public static long midPoint(long left, long right) {
        // using BigInteger to avoid long overflow in intermediate operations
        BigInteger l = BigInteger.valueOf(left);
        BigInteger r = BigInteger.valueOf(right);
        BigInteger midPoint;

        if (l.compareTo(r) < 0) {
            BigInteger sum = l.add(r);
            midPoint = sum.shiftRight(1);
        } else // wrapping case
        {
            BigInteger max = BigInteger.valueOf(Long.MAX_VALUE);
            BigInteger min = BigInteger.valueOf(Long.MIN_VALUE);
            // length of range we're bisecting is (R - min) + (max - L)
            // so we add that to L giving
            // L + ((R - min) + (max - L) / 2) = (L + R + max - min) / 2
            midPoint = (max.subtract(min).add(l).add(r)).shiftRight(1);
            if (midPoint.compareTo(max) > 0)
                midPoint = min.add(midPoint.subtract(max));
        }

        return midPoint.longValue();
    }

    public static <T> BinaryOperator<T> throwingMerger() {
        return (u, v) -> {
            throw new IllegalStateException(String.format("Duplicate key %s", u));
        };
    }

    public static MerkleTree createEmptyTree(@NotNull Range<Long> range, int depth) {
        MerkleTree tree = new MerkleTree(range, depth);
        MerkleTree.fillTree(tree, ImmutableSortedMap.of());
        tree.calculate();
        return tree;
    }

    public static MerkleTree createTree(@NotNull Range<Long> range, int depth, @NotNull SortedMap<Long, Block> blocks) {
        MerkleTree tree = new MerkleTree(range, depth);
        MerkleTree.fillTree(tree, blocks);
        tree.calculate();
        return tree;
    }
}
