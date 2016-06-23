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

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Striped;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.blobkeeper.cluster.configuration.ClusterPropertiesConfiguration;
import io.blobkeeper.cluster.domain.*;
import io.blobkeeper.cluster.util.ClusterUtils;
import io.blobkeeper.file.service.DiskService;
import io.blobkeeper.file.service.PartitionService;
import io.blobkeeper.index.domain.Partition;
import org.jetbrains.annotations.NotNull;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.size;
import static com.google.common.util.concurrent.Striped.semaphore;
import static io.blobkeeper.cluster.domain.Command.REPLICATION_REQUEST;
import static org.joda.time.DateTime.now;
import static org.joda.time.DateTimeZone.UTC;

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

    @Inject
    private ClusterPropertiesConfiguration propertiesConfiguration;

    private final Striped<Semaphore> semaphores = semaphore(16, 1);

    private final ScheduledExecutorService replicationTaskExecutor =
            Executors.newScheduledThreadPool(
                    32,
                    new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("RepairWorker-%d")
                            .build()
            );

    public void init() {
        replicationTaskExecutor.scheduleWithFixedDelay(
                new RepairTask(),
                getInitialDelaySeconds(),
                DateTimeConstants.SECONDS_PER_DAY,
                TimeUnit.SECONDS
        );
    }

    @Override
    public void repair(boolean allPartitions) {
        diskService.getDisks().forEach(disk -> repair(disk, allPartitions));
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
    public void repair(int disk, boolean allPartitions) {
        Semaphore semaphore = semaphores.get(disk);

        try {
            boolean acquired = semaphore.tryAcquire(5, TimeUnit.SECONDS);
            if (!acquired) {
                log.info("Repairing already in progress");
                return;
            }

            log.info("Repair of disk {} started", disk);
            membershipService.getMaster().ifPresent(
                    master -> {
                        Partition active = partitionService.getActivePartition(disk);
                        checkNotNull(active, "Active partition is required!");

                        log.info("Replication starts, master node is {}", master);
                        ReplicationTask replicationTask = new ReplicationTask(disk, allPartitions);

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
                    }
            );
        } catch (Exception e) {
            log.error("Can't repair cluster", e);
            semaphore.release();
        }
    }

    private Map<Integer, MerkleTreeInfo> getExpectedData(int disk) {
        List<Partition> partitions = partitionService.getPartitions(disk);
        return clusterUtils.getExpectedTrees(disk, partitions);
    }

    private int getInitialDelaySeconds() {
        int repairHour = propertiesConfiguration.getRepairTimeHour();
        int currentHour = now(UTC).getHourOfDay();

        int delay = 0;

        if (repairHour < currentHour) {
            delay = 24 - currentHour - repairHour;
        }

        if (repairHour > currentHour) {
            delay = repairHour - currentHour;
        }

        return delay * 60 * 60 + 30;
    }

    private class RepairTask implements Runnable {
        @Override
        public void run() {
            try {
                log.info("Repair all partitions started");

                repair(true);

                log.info("Repair all partitions finished");
            } catch (Exception e) {
                log.error("Can't start periodic repair", e);
            }
        }
    }

    private class ReplicationTask implements Runnable {
        private final int disk;
        private final Partition active;
        private final boolean allPartitions;

        ReplicationTask(int disk, boolean allPartitions) {
            this.disk = disk;
            this.active = partitionService.getActivePartition(disk);
            this.allPartitions = allPartitions;
        }

        @Override
        public void run() {
            log.debug("Getting file list");

            // TODO: add logging for filtered blobs
            // log.debug("Replication of {} is not required for node {}", replicatingBlob, membershipService.getSelfNode());

            Map<Integer, MerkleTreeInfo> expectedData;
            if (allPartitions) {
                expectedData = getExpectedData(disk);
            } else {
                expectedData = ImmutableMap.of();
            }

            try {
                Stream.concat(
                        expectedData.values()
                                .stream()
                                .map(new TreeToRepairRequest()),
                        // active partition always replicates
                        Stream.of(getForActive())
                ).forEach(new DifferenceConsumer());
            } catch (Exception e) {
                log.error("Can't replicate file", e);
                throw new ReplicationServiceException(e);
            }
        }

        private RepairRequest getForActive() {
            DifferenceInfo differenceInfo = new DifferenceInfo();
            differenceInfo.setDisk(active.getDisk());
            differenceInfo.setPartition(active.getId());
            differenceInfo.setCompletelyDifferent(true);

            return new RepairRequest.Builder()
                    .diff(differenceInfo)
                    .withNode(membershipService.getNodeForRepair(true))
                    .build();
        }

        private class DifferenceConsumer implements Consumer<RepairRequest> {
            @Override
            public void accept(RepairRequest repairRequest) {
                if (!repairRequest.getRepairNode().isPresent()) {
                    log.error("No repair node for {}", repairRequest);
                    return;
                }

                if (repairRequest.getDifferenceInfo().isNoDiff()) {
                    log.info("No diff {}", repairRequest.getDifferenceInfo());
                    return;
                }

                JChannel channel = membershipService.getChannel();
                log.info("Replication request sending for file {} to node {}", repairRequest.getDifferenceInfo(), repairRequest.getRepairNode().get());
                try {
                    Message message = ClusterUtils.createMessage(
                            membershipService.getSelfNode().getAddress(),
                            repairRequest.getRepairNode().get().getAddress(),
                            repairRequest.getDifferenceInfo(),
                            new CustomMessageHeader(REPLICATION_REQUEST)
                    );

                    channel.send(message);
                } catch (Exception e) {
                    log.error("Can't request replication for file {}", repairRequest.getDifferenceInfo(), e);
                }
            }
        }

        private class TreeToRepairRequest implements Function<MerkleTreeInfo, RepairRequest> {
            /**
             * @return non-empty difference, in case of trees are different between remote and local host
             * Remote tree and expected tree (from blob index) must be equal.
             */
            @Override
            public RepairRequest apply(MerkleTreeInfo expected) {
                boolean isActive = expected.getPartition() == active.getId();
                boolean isSelfNodeMaster = membershipService.isMaster();

                Optional<Node> remoteNode = membershipService.getNodeForRepair(isActive);

                log.info("Disk, partition is {}, {}; Remote node is {}", expected.getDisk(), expected.getPartition(), remoteNode);

                RepairRequest.Builder requestBuilder = new RepairRequest.Builder()
                        .withNode(remoteNode);

                DifferenceInfo noDiff = new DifferenceInfo();
                noDiff.setDifference(ImmutableList.of());
                noDiff.setDisk(expected.getDisk());
                noDiff.setPartition(expected.getPartition());

                // FIXME: repair master?
                // no need replicate active partition to master
                if (isSelfNodeMaster && isActive) {
                    log.info("Master {} knows more about active partition {}, request node {}", membershipService.getMaster(), active, remoteNode);
                    return requestBuilder.diff(noDiff)
                            .build();
                }

                boolean isDstNodeMaster = remoteNode.equals(membershipService.getMaster());

                // check active node only on master
                if (!isDstNodeMaster && isActive) {
                    log.info("Only master {} knows about active partition {}, request node {}", membershipService.getMaster(), active, remoteNode);
                    return requestBuilder.diff(noDiff)
                            .build();
                }

                // TODO: add cache?
                DifferenceInfo local = membershipService.getDifference(expected);

                if (local != null && local.getDifference().isEmpty()) {
                    log.debug("Local file tree is equals to the expected for file {}", local);
                    return requestBuilder.diff(noDiff)
                            .build();
                }

                return requestBuilder.diff(local)
                        .build();
            }
        }
    }

    private static class RepairRequest {
        private final DifferenceInfo differenceInfo;
        private final Optional<Node> repairNode;

        RepairRequest(Builder builder) {
            this.differenceInfo = builder.differenceInfo;
            this.repairNode = builder.repairNode;
        }

        DifferenceInfo getDifferenceInfo() {
            return differenceInfo;
        }

        public Optional<Node> getRepairNode() {
            return repairNode;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("differenceInfo", differenceInfo)
                    .add("repairNode", repairNode)
                    .toString();
        }

        static class Builder {
            private DifferenceInfo differenceInfo;
            private Optional<Node> repairNode;

            Builder diff(@NotNull DifferenceInfo differenceInfo) {
                this.differenceInfo = differenceInfo;
                return this;
            }

            public Builder withNode(@NotNull Optional<Node> repairNode) {
                this.repairNode = repairNode;
                return this;
            }

            public RepairRequest build() {
                return new RepairRequest(this);
            }
        }
    }
}
