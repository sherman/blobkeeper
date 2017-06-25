package io.blobkeeper.cluster.service;

/*
 * Copyright (C) 2015-2016 by Denis M. Gabaydulin
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
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.blobkeeper.cluster.domain.*;
import io.blobkeeper.common.configuration.MetricModule;
import io.blobkeeper.common.configuration.RootModule;
import io.blobkeeper.common.util.Block;
import io.blobkeeper.common.util.BlockElt;
import io.blobkeeper.common.util.MerkleTree;
import io.blobkeeper.common.util.Utils;
import io.blobkeeper.file.service.DiskService;
import io.blobkeeper.file.service.FileListService;
import io.blobkeeper.file.service.FileStorage;
import io.blobkeeper.file.service.PartitionService;
import io.blobkeeper.index.dao.IndexDao;
import io.blobkeeper.index.dao.PartitionDao;
import io.blobkeeper.index.domain.Partition;
import io.blobkeeper.index.service.IndexService;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.fork.ForkChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Arrays;
import java.util.Optional;

import static com.google.common.collect.Range.closedOpen;
import static java.lang.Thread.sleep;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

@Guice(modules = {RootModule.class, RepairServiceTest.Mocks.class, MetricModule.class})
public class RepairServiceTest {
    private static final Logger log = LoggerFactory.getLogger(RepairServiceTest.class);

    @Inject
    private FileStorage fileStorage;

    @Inject
    private IndexService indexService;

    @Inject
    private ReplicationClientService replicationClientService;

    @Inject
    private ClusterMembershipService clusterMembershipService;

    @Inject
    private ForkChannel channel;

    @Inject
    private ReplicationHandlerService replicationHandlerService;

    @Inject
    private IndexDao indexDao;

    @Inject
    private PartitionDao partitionDao;

    @Inject
    private PartitionService partitionService;

    @Inject
    private RepairService repairService;

    @Inject
    private FileListService fileListService;

    @Inject
    private DiskService diskService;

    @Test
    public void replicateNotEqualsAndNonActivePartitionsFromAnyNode() throws Exception {
        Address masterAddress = mock(Address.class);
        Address slaveAddress1 = mock(Address.class);
        Address slaveAddress2 = mock(Address.class);
        Node master = new Node(Role.MASTER, masterAddress, System.currentTimeMillis());
        Node slave1 = new Node(Role.SLAVE, slaveAddress1, System.currentTimeMillis());
        Node slave2 = new Node(Role.SLAVE, slaveAddress2, System.currentTimeMillis());
        when(clusterMembershipService.getMaster()).thenReturn(Optional.of(master));
        when(clusterMembershipService.getSelfNode()).thenReturn(slave1);

        when(clusterMembershipService.getMessageChannel()).thenReturn(channel);
        when(clusterMembershipService.getNodeForRepair(eq(true))).thenReturn(Optional.empty());
        when(clusterMembershipService.getNodeForRepair(eq(false))).thenReturn(Optional.of(slave2));
        View view = mock(View.class);
        when(channel.getView()).thenReturn(view);
        when(view.getMembers()).thenReturn(ImmutableList.of(masterAddress, slaveAddress1, slaveAddress2));

        Partition partition1 = new Partition(0, 0);
        partition1.setTree(Utils.createEmptyTree(closedOpen(0L, 100L), MerkleTree.MAX_LEVEL));

        Partition partition2 = new Partition(0, 1);

        when(partitionService.getPartitions(eq(0))).thenReturn(ImmutableList.of(partition1, partition2));
        when(partitionService.getActivePartition(eq(0))).thenReturn(partition2);

        MerkleTree masterTree = Utils.createTree(
                closedOpen(0L, 100L),
                32,
                ImmutableSortedMap.of(42L, new Block(1L, Arrays.asList(new BlockElt(1, 0, 2, 3, 4))))
        );

        MerkleTreeInfo masterInfo = new MerkleTreeInfo();
        masterInfo.setTree(masterTree);
        masterInfo.setDisk(0);
        masterInfo.setPartition(0);

        // index already exists
        MerkleTree slaveTree = Utils.createTree(
                closedOpen(0L, 100L),
                32,
                ImmutableSortedMap.of()
        );

        MerkleTreeInfo slaveInfo = new MerkleTreeInfo();
        slaveInfo.setTree(slaveTree);
        slaveInfo.setDisk(0);
        slaveInfo.setPartition(0);

        DifferenceInfo partitionInfo = new DifferenceInfo();
        partitionInfo.setDisk(0);
        partitionInfo.setPartition(0);
        partitionInfo.setDifference(MerkleTree.difference(masterInfo.getTree(), slaveInfo.getTree()));

        when(clusterMembershipService.getMerkleTreeInfo(eq(slaveAddress2), eq(0), eq(0))).thenReturn(masterInfo);
        when(clusterMembershipService.getMerkleTreeInfo(eq(slaveAddress1), eq(0), eq(0))).thenReturn(slaveInfo);
        when(clusterMembershipService.getDifference(eq(slaveAddress1), eq(0), eq(0))).thenReturn(partitionInfo);
        when(clusterMembershipService.getDifference(eq(slaveInfo))).thenReturn(partitionInfo);

        when(diskService.getDisks()).thenReturn(ImmutableList.of(0));

        repairService.repair(true);

        sleep(100);

        verify(channel).send(argThat(new MessageMatcher(slaveAddress2, slaveAddress1, partitionInfo)));
    }

    @Test
    public void replicateActivePartition() throws Exception {
        Address masterAddress = mock(Address.class);
        Address slaveAddress = mock(Address.class);
        Node master = new Node(Role.MASTER, masterAddress, System.currentTimeMillis());
        Node slave = new Node(Role.SLAVE, slaveAddress, System.currentTimeMillis());
        when(clusterMembershipService.getMaster()).thenReturn(Optional.of(master));
        when(clusterMembershipService.getSelfNode()).thenReturn(slave);

        when(clusterMembershipService.getMessageChannel()).thenReturn(channel);
        when(clusterMembershipService.getNodeForRepair(eq(true))).thenReturn(Optional.of(master));
        View view = mock(View.class);
        when(channel.getView()).thenReturn(view);
        when(view.getMembers()).thenReturn(ImmutableList.of(masterAddress, slaveAddress));

        Partition partition = new Partition(0, 0);
        when(partitionService.getPartitions(eq(0))).thenReturn(ImmutableList.of(partition));
        when(partitionService.getActivePartition(eq(0))).thenReturn(partition);

        when(diskService.getDisks()).thenReturn(ImmutableList.of(0));

        repairService.repair(true);

        sleep(100);

        verify(channel, only()).send(any(Message.class));
    }

    @Test
    public void replicateNotEqualsPartitions() throws Exception {
        Address masterAddress = mock(Address.class);
        Address slaveAddress = mock(Address.class);
        Node master = new Node(Role.MASTER, masterAddress, System.currentTimeMillis());
        Node slave = new Node(Role.SLAVE, slaveAddress, System.currentTimeMillis());
        when(clusterMembershipService.getMaster()).thenReturn(Optional.of(master));
        when(clusterMembershipService.getSelfNode()).thenReturn(slave);

        when(clusterMembershipService.getMessageChannel()).thenReturn(channel);
        when(clusterMembershipService.getNodeForRepair(eq(true))).thenReturn(Optional.of(master));
        when(clusterMembershipService.getNodeForRepair(eq(false))).thenReturn(Optional.of(master));
        View view = mock(View.class);
        when(channel.getView()).thenReturn(view);
        when(view.getMembers()).thenReturn(ImmutableList.of(masterAddress, slaveAddress));

        Partition partition1 = new Partition(0, 0);
        partition1.setTree(Utils.createEmptyTree(closedOpen(0L, 100L), MerkleTree.MAX_LEVEL));

        Partition partition2 = new Partition(0, 1);

        when(partitionService.getPartitions(eq(0))).thenReturn(ImmutableList.of(partition1, partition2));
        when(partitionService.getActivePartition(eq(0))).thenReturn(partition2);

        MerkleTree masterTree = Utils.createTree(
                closedOpen(0L, 100L),
                32,
                ImmutableSortedMap.of(42L, new Block(1L, Arrays.asList(new BlockElt(1, 0, 2, 3, 4))))
        );

        MerkleTreeInfo masterInfo = new MerkleTreeInfo();
        masterInfo.setTree(masterTree);
        masterInfo.setDisk(0);
        masterInfo.setPartition(0);

        // index already exists
        MerkleTree slaveTree = Utils.createTree(
                closedOpen(0L, 100L),
                32,
                ImmutableSortedMap.of()
        );

        MerkleTreeInfo slaveInfo = new MerkleTreeInfo();
        slaveInfo.setTree(slaveTree);
        slaveInfo.setDisk(0);
        slaveInfo.setPartition(0);

        DifferenceInfo partitionInfo = new DifferenceInfo();
        partitionInfo.setDisk(0);
        partitionInfo.setPartition(0);
        partitionInfo.setDifference(MerkleTree.difference(masterInfo.getTree(), slaveInfo.getTree()));

        DifferenceInfo activePartitionInfo = new DifferenceInfo();
        activePartitionInfo.setDisk(0);
        activePartitionInfo.setPartition(1);
        activePartitionInfo.setCompletelyDifferent(true);

        when(clusterMembershipService.getMerkleTreeInfo(eq(masterAddress), eq(0), eq(0))).thenReturn(masterInfo);
        when(clusterMembershipService.getMerkleTreeInfo(eq(slaveAddress), eq(0), eq(0))).thenReturn(slaveInfo);
        when(clusterMembershipService.getDifference(eq(slaveAddress), eq(0), eq(0))).thenReturn(partitionInfo);
        when(clusterMembershipService.getDifference(eq(slaveInfo))).thenReturn(partitionInfo);

        when(diskService.getDisks()).thenReturn(ImmutableList.of(0));

        repairService.repair(true);

        sleep(100);

        verify(channel).send(argThat(new MessageMatcher(masterAddress, slaveAddress, activePartitionInfo)));
        verify(channel).send(argThat(new MessageMatcher(masterAddress, slaveAddress, partitionInfo)));
    }

    @BeforeClass
    private void init() {
        initMocks(this);
    }

    @BeforeMethod
    private void clear() {
        reset(fileStorage, indexService, replicationClientService, clusterMembershipService, channel, fileListService);
    }

    private static class MessageMatcher implements Matcher<Message> {
        private final Address dst;
        private final Address src;
        private final DifferenceInfo info;

        public MessageMatcher(Address dst, Address src, DifferenceInfo info) {
            this.dst = dst;
            this.src = src;
            this.info = info;
        }

        @Override
        public boolean matches(Object address) {
            Message message = ((Message) address);
            return
                    dst.equals(message.getDest())
                            && src.equals(message.getSrc())
                            && info.equals(message.getObject());
        }

        @Override
        public void _dont_implement_Matcher___instead_extend_BaseMatcher_() {
        }

        @Override
        public void describeTo(Description description) {
        }
    }

    public static class Mocks extends AbstractModule {
        @Provides
        @Singleton
        PartitionDao partitionDao() {
            return mock(PartitionDao.class);
        }

        @Provides
        @Singleton
        IndexDao indexDao() {
            return mock(IndexDao.class);
        }

        @Provides
        @Singleton
        IndexService indexService() {
            return mock(IndexService.class);
        }

        @Provides
        @Singleton
        FileStorage fileStorage() {
            return mock(FileStorage.class);
        }

        @Provides
        @Singleton
        ClusterMembershipService clusterMembershipService() {
            return mock(ClusterMembershipService.class);
        }

        @Provides
        @Singleton
        ForkChannel jChannel() {
            return mock(ForkChannel.class);
        }

        @Provides
        @Singleton
        ReplicationHandlerService replicationHandlerService() {
            return mock(ReplicationHandlerService.class);
        }

        @Provides
        @Singleton
        PartitionService partitionService() {
            return mock(PartitionService.class);
        }

        @Provides
        @Singleton
        FileListService fileListService() {
            return mock(FileListService.class);
        }

        @Provides
        @Singleton
        DiskService diskService() {
            return mock(DiskService.class);
        }

        @Provides
        @Singleton
        ReplicationClientService replicationClientService() {
            return mock(ReplicationClientService.class);
        }

        @Override
        protected void configure() {
        }
    }
}

