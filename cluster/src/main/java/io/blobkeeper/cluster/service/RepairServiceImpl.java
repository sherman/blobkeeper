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
import com.google.common.util.concurrent.Striped;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.blobkeeper.cluster.domain.*;
import io.blobkeeper.cluster.util.ClusterUtils;
import io.blobkeeper.file.service.DiskService;
import io.blobkeeper.file.service.PartitionService;
import io.blobkeeper.index.domain.Partition;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.size;
import static com.google.common.util.concurrent.Striped.semaphore;
import static io.blobkeeper.cluster.domain.Command.REPLICATION_REQUEST;
import static io.blobkeeper.cluster.domain.CustomMessageHeader.CUSTOM_MESSAGE_HEADER;

@Singleton
public class RepairServiceImpl implements RepairService {
    private static final Logger log = LoggerFactory.getLogger(RepairServiceImpl.class);

    @Inject
    private PartitionService partitionService;

    @Inject
    private DiskService diskService;

    @Inject
    private ClusterMembershipService membershipService;

    @Inject
    private ClusterUtils clusterUtils;

    private final Striped<Semaphore> semaphores = semaphore(16, 1);

    private final ExecutorService replicationTaskExecutor =
            Executors.newFixedThreadPool(
                    32,
                    new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("RepairWorker-%d")
                            .build()
            );

    @Override
    public void repair() {
        diskService.getDisks().forEach(this::repair);
    }

    @Override
    public boolean isRepairInProgress() {
        List<Integer> disks = diskService.getDisks();
        return size(semaphores.bulkGet(disks)) < disks.size();
    }

    /**
     * Repairs files on the self node.
     * <p>
     * Before the repair has been started, it compares the files  between remote node and target node.
     * If the files are not equal and remote node has correct files,
     * the target node requests a replication of corrupted file from the remote node.
     * <p>
     * The replication process is pretty simple.
     * The remote node sends the request blob (block by block) to the target node.
     */
    @Override
    public void repair(int disk) {
        Semaphore semaphore = semaphores.get(disk);

        try {
            boolean acquired = semaphore.tryAcquire(5, TimeUnit.SECONDS);
            if (!acquired) {
                log.info("Repairing already in progress");
                return;
            }

            log.info("Repair of disk {} started", disk);
            Node masterNode = membershipService.getMaster();
            checkNotNull(masterNode, "Master node is required!");

            Partition active = partitionService.getActivePartition(disk);
            checkNotNull(active, "Active partition is required!");

            log.info("Replication starts for master node {}", masterNode);
            ReplicationTask replicationTask = new ReplicationTask(masterNode.getAddress(), disk);

            CompletableFuture.<Void>runAsync(replicationTask, replicationTaskExecutor)
                    .thenAcceptAsync(aVoid -> {
                        log.info("Repair of disk {} finished", disk);
                        semaphore.release();
                    }, replicationTaskExecutor)
                    .exceptionally(throwable -> {
                        log.error("Can't repair cluster", throwable);
                        semaphore.release();
                        return null;
                    });
        } catch (Exception e) {
            log.error("Can't repair cluster", e);
            semaphore.release();
        }
    }

    private Map<Integer, MerkleTreeInfo> getExpectedData(int disk) {
        List<Partition> partitions = partitionService.getPartitions(disk);
        return clusterUtils.getExpectedTrees(disk, partitions);
    }

    private class ReplicationTask implements Runnable {
        private final Address remoteNode;
        private final int disk;
        private Partition active;

        public ReplicationTask(
                Address remoteNode,
                int disk
        ) {
            this.remoteNode = remoteNode;
            this.disk = disk;
            this.active = partitionService.getActivePartition(disk);
        }

        @Override
        public void run() {
            log.debug("Getting file list from node {}", remoteNode);

            // TODO: add logging for filtered blobs
            // log.debug("Replication of {} is not required for node {}", replicatingBlob, membershipService.getSelfNode());
            Map<Integer, MerkleTreeInfo> expectedData = getExpectedData(disk);

            try {
                Stream.concat(
                        expectedData.values()
                                .stream()
                                .map(new TreeToDifference()),
                        // active partition always replicates
                        Stream.of(getForActive())
                ).forEach(new DifferenceConsumer(remoteNode));
            } catch (Exception e) {
                log.error("Can't replicate file", e);
                throw new ReplicationServiceException(e);
            }
        }

        private DifferenceInfo getForActive() {
            DifferenceInfo differenceInfo = new DifferenceInfo();
            differenceInfo.setDisk(active.getDisk());
            differenceInfo.setPartition(active.getId());
            differenceInfo.setCompletelyDifferent(true);
            return differenceInfo;
        }

        private class DifferenceConsumer implements Consumer<DifferenceInfo> {

            private final Address node;

            DifferenceConsumer(Address node) {
                this.node = node;
            }

            @Override
            public void accept(DifferenceInfo differenceInfo) {
                if (differenceInfo.getDifference().isEmpty() && !differenceInfo.isCompletelyDifferent()) {
                    log.info("No diff {}", differenceInfo);
                    return;
                }

                JChannel channel = membershipService.getChannel();
                log.debug("Replication request sending for file {} to node {}", differenceInfo, node);
                try {
                    channel.send(createReplicationRequestMessage(node, differenceInfo));
                } catch (Exception e) {
                    log.error("Can't request replication for file {}", differenceInfo, e);
                }
            }

            private Message createReplicationRequestMessage(Address dest, DifferenceInfo differenceInfo) {
                Message message = new Message();
                message.setDest(dest);
                message.setSrc(membershipService.getSelfNode().getAddress());
                message.putHeader(CUSTOM_MESSAGE_HEADER, new CustomMessageHeader(REPLICATION_REQUEST));
                message.setObject(differenceInfo);
                return message;
            }
        }

        private class TreeToDifference implements Function<MerkleTreeInfo, DifferenceInfo> {
            /**
             * @return non-empty difference, in case of trees are different between remote and local host
             * Remote tree and expected tree (from blob index) must be equal.
             */
            @Override
            public DifferenceInfo apply(MerkleTreeInfo expected) {
                boolean isActive = expected.getPartition() == active.getId();
                boolean isSelfNodeMaster = membershipService.isMaster();

                DifferenceInfo noDiff = new DifferenceInfo();
                noDiff.setDifference(ImmutableList.of());
                noDiff.setDisk(expected.getDisk());
                noDiff.setPartition(expected.getPartition());

                // no need replicate active partition to master
                if (isSelfNodeMaster && isActive) {
                    log.info("Master {} knows more about active partition {}, request node {}", membershipService.getMaster(), active, remoteNode);
                    return noDiff;
                }

                boolean isDstNodeMaster = remoteNode.equals(membershipService.getMaster().getAddress());

                // check active node only on master
                if (!isDstNodeMaster) {
                    log.info("Only master {} knows about active partition {}, request node {}", membershipService.getMaster(), active, remoteNode);
                    return noDiff;
                }

                // TODO: add cache?
                DifferenceInfo local = membershipService.getDifference(expected);

                if (local != null && local.getDifference().isEmpty()) {
                    log.debug("Local file tree is equals to the expected for file {}", local);
                    return noDiff;
                }

                return local;
            }
        }
    }
}
