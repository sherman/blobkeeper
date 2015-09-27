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

import java.io.Serializable;

import static io.blobkeeper.common.util.Utils.xor;
import static java.lang.String.format;

public class BranchNode extends HashableNode implements Serializable {
    private static final long serialVersionUID = -833670422341527046L;

    private final long midPoint;
    private final HashableNode left;
    private final HashableNode right;

    BranchNode(long midPoint, HashableNode left, HashableNode right) {
        this.midPoint = midPoint;
        this.left = left;
        this.right = right;
    }

    HashableNode getLeft() {
        return left;
    }

    HashableNode getRight() {
        return right;
    }

    /**
     * Recursively calculate hashes
     */
    @Override
    HashableNode calculate() {
        if (hash == null) {
            HashableNode leftNode = left.calculate();
            HashableNode rightNode = right.calculate();

            hash = xor(leftNode.hash, rightNode.hash);
            length = leftNode.length + rightNode.length;
            blocks = leftNode.blocks + rightNode.blocks;
        }

        return this;
    }

    @Override
    public String toString() {
        return format("#<Branch %s %s>", midPoint, super.toString());
    }
}
