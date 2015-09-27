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

import io.blobkeeper.common.util.Block;
import io.blobkeeper.common.util.MerkleTree;
import io.blobkeeper.common.util.Utils;
import io.blobkeeper.index.domain.IndexElt;
import io.blobkeeper.index.domain.Partition;
import io.blobkeeper.index.service.IndexService;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.blobkeeper.common.util.MerkleTree.MAX_LEVEL;
import static java.util.stream.Collectors.toMap;

@Singleton
public class IndexUtils {
    private static final Logger log = LoggerFactory.getLogger(IndexUtils.class);

    @Inject
    private IndexService indexService;

    public long getOffset(@NotNull List<IndexElt> elts) {
        checkNotNull(elts, "Elts are required!");

        return elts.stream()
                .mapToLong(IndexElt::getLength)
                .sum();
    }

    @NotNull
    public MerkleTree buildMerkleTree(@NotNull Partition partition) {
        List<IndexElt> elts = indexService.getListByPartition(partition);

        SortedMap<Long, Block> blocks = elts.stream()
                .collect(toMap(
                                IndexElt::getId,
                                indexElt -> new Block(indexElt.getId(), indexElt.getOffset(), indexElt.getLength(), indexElt.getCrc()),
                                Utils.throwingMerger(),
                                TreeMap::new)
                );

        MerkleTree tree = new MerkleTree(indexService.getMinMaxRange(partition), MAX_LEVEL);
        MerkleTree.fillTree(tree, blocks);
        tree.calculate();

        return tree;
    }
}
