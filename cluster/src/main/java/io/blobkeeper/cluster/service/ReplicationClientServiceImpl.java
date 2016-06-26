package io.blobkeeper.cluster.service;

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
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import io.blobkeeper.cluster.configuration.ClusterPropertiesConfiguration;
import io.blobkeeper.cluster.domain.*;
import io.blobkeeper.cluster.util.ClusterUtils;
import io.blobkeeper.cluster.util.ReplicationStatistic;
import io.blobkeeper.common.util.LeafNode;
import io.blobkeeper.common.util.MerkleTree;
import io.blobkeeper.file.domain.File;
import io.blobkeeper.file.domain.ReplicationFile;
import io.blobkeeper.file.service.FileListService;
import io.blobkeeper.file.service.PartitionService;
import io.blobkeeper.file.util.FileUtils;
import io.blobkeeper.file.util.IndexEltOffsetComparator;
import io.blobkeeper.index.domain.IndexElt;
import io.blobkeeper.index.domain.Partition;
import io.blobkeeper.index.service.IndexService;
import io.blobkeeper.index.service.NoIndexRangeException;
import org.jetbrains.annotations.NotNull;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.blobkeeper.cluster.domain.Command.FILE;
import static io.blobkeeper.cluster.util.ClusterUtils.createMessage;
import static io.blobkeeper.index.domain.PartitionState.NEW;
import static java.lang.Thread.sleep;
import static java.util.Collections.sort;
import static java.util.concurrent.CompletableFuture.runAsync;

@Singleton
public class ReplicationClientServiceImpl implements ReplicationClientService {
    private static final Logger log = LoggerFactory.getLogger(ReplicationClientServiceImpl.class);

    @Inject
    private ClusterMembershipService membershipService;

    @Inject
    private FileListService fileListService;

    @Inject
    private IndexService indexService;

    @Inject
    private ClusterPropertiesConfiguration configuration;

    @Inject
    private ClusterUtils clusterUtils;

    @Inject
    private PartitionService partitionService;

    @Inject
    private ReplicationStatistic replicationStatistic;

    @Override
    public void replicate(@NotNull ReplicationFile file) {
        if (log.isTraceEnabled()) {
            log.trace("Replicating file {}", file);
        }

        Optional<Node> masterNode = membershipService.getMaster();
        checkNotNull(masterNode.isPresent(), "Master node is required!");

        membershipService.getNodes()
                .stream()
                .filter(node -> !(node.equals(masterNode.get()) || node.equals(membershipService.getSelfNode())))
                .forEach(node -> runAsync(() -> replicate(file, node.getAddress())));
    }

    @Override
    public void replicate(@NotNull ReplicationFile file, @NotNull Address dst) {
        JChannel channel = membershipService.getChannel();
        log.trace("Replication packet sending for {}", dst);
        try {
            Message message = createMessage(
                    membershipService.getSelfNode().getAddress(),
                    dst,
                    file,
                    new CustomMessageHeader(FILE)
            );

            channel.send(message);
        } catch (Exception e) {
            log.error("Can't replicate file", e);
            throw new ReplicationServiceException(e);
        } finally {
            replicationStatistic.onReplicationElt();
        }
    }

    // TODO: prevent simultaneous replication of multiple disk partitions (add disk lock?)
    @Override
    public void replicate(@NotNull DifferenceInfo differenceInfo, @NotNull Address dst) {
        replicationStatistic.onReplicationRequest();

        Partition partition = partitionService.getById(differenceInfo.getDisk(), differenceInfo.getPartition());

        if (!isReplicationAvailable(partition, differenceInfo)) {
            return;
        }

        // TODO: calculate what types are different instead whole range
        RangeMap<Long, LeafNode> nodes = TreeRangeMap.create();

        differenceInfo.getDifference().stream()
                .forEach(diff -> nodes.put(diff.getRange(), diff));

        log.info("File will be synced {}, dst node {}", differenceInfo, dst);

        File file = null;
        int sentElts = 1;
        try {
            file = fileListService.getFile(differenceInfo.getDisk(), differenceInfo.getPartition());
            if (null == file) {
                log.error("Can't replicate blob file {}, dst node {}", differenceInfo, dst);
                return;
            }

            List<IndexElt> elts = new ArrayList<>(indexService.getListByPartition(partition));

            // sort it by offset, to read file consequentially
            sort(elts, new IndexEltOffsetComparator());

            for (IndexElt elt : elts) {
                // not in diff
                if (null == nodes.get(elt.getId()) && !differenceInfo.isCompletelyDifferent()) {
                    continue;
                }

                // pause to send new files
                if (sentElts % configuration.getReplicationMaxFiles() == 0) {
                    sleep(configuration.getReplicationDelay());
                }

                ByteBuffer buffer;
                try {
                    buffer = FileUtils.readFile(file, elt.getOffset(), elt.getLength());
                } catch (Exception e) {
                    log.error("Can't read data for index {}, required {}", elt, elt.getLength(), e);
                    continue;
                }

                byte[] bufferBytes = new byte[buffer.remaining()];
                buffer.get(bufferBytes);

                if (bufferBytes.length < elt.getLength()) {
                    log.error("Can't replicate elt {}", elt);
                    continue;
                }

                ReplicationFile replicationFile = new ReplicationFile(elt.getDiskIndexElt(), bufferBytes);
                try {
                    replicate(replicationFile, dst);
                } catch (ReplicationServiceException e) {
                    log.error("Can't replicate file {}", elt, e);

                }
            }
        } catch (Exception e) {
            log.error("Can't replicate block {}", partition, e);
        } finally {
            if (null != file) {
                try {
                    file.close();
                } catch (Exception ignored) {
                }
            }
        }
    }

    private boolean isReplicationAvailable(Partition partition, DifferenceInfo differenceInfo) {
        return partition.getState() == NEW && (differenceInfo.isCompletelyDifferent() || isExpectedMerkleTree(partition));
    }

    private boolean isExpectedMerkleTree(@NotNull Partition partition) {
        MerkleTreeInfo local;
        try {
            local = membershipService.getMerkleTreeInfo(
                    membershipService.getSelfNode().getAddress(),
                    partition.getDisk(),
                    partition.getId()
            );
        } catch (NoIndexRangeException e) {
            log.info("No elements in index", e);
            return false;
        }

        // no file
        if (null == local) {
            log.error("No file");
            return false;
        }

        Map<Integer, MerkleTreeInfo> expectedData = clusterUtils.getExpectedTrees(partition.getDisk(), ImmutableList.of(partition));

        MerkleTreeInfo treeInfo = expectedData.get(partition.getId());
        if (null == treeInfo) {
            log.error("No tree info (no index?)");
            return false;
        }

        MerkleTree expectedTree = treeInfo.getTree();
        boolean treeIsExpected = MerkleTree.difference(expectedTree, local.getTree()).isEmpty();

        if (!treeIsExpected) {
            log.error(
                    "Can't replicate file {}, tree on master node {} is not equals to the expected {}",
                    partition,
                    local.getTree(),
                    expectedTree
            );
        }

        return treeIsExpected;
    }
}
