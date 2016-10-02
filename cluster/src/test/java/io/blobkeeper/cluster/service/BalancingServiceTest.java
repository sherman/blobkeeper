package io.blobkeeper.cluster.service;

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
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.blobkeeper.common.configuration.MetricModule;
import io.blobkeeper.common.configuration.RootModule;
import io.blobkeeper.file.configuration.FileConfiguration;
import io.blobkeeper.file.configuration.FileModule;
import io.blobkeeper.file.service.*;
import io.blobkeeper.index.dao.IndexDao;
import io.blobkeeper.index.dao.PartitionDao;
import io.blobkeeper.index.domain.Partition;
import io.blobkeeper.index.service.IndexService;
import org.jgroups.JChannel;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import javax.inject.Singleton;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Guice(modules = {RootModule.class, BalancingServiceTest.Mocks.class, MetricModule.class})
public class BalancingServiceTest {

    @Inject
    private BalancingService balancingService;

    @Inject
    private DiskService diskService;

    @Inject
    private PartitionService partitionService;

    @Test
    public void noDisks() {
        when(diskService.getDisks()).thenReturn(ImmutableList.of());

        assertTrue(balancingService.getMovePartitions().isEmpty());
    }

    @Test
    public void noNeedBalance() {
        when(diskService.getDisks()).thenReturn(ImmutableList.of(0, 1, 2));
        when(partitionService.getPartitions(0)).thenReturn(ImmutableList.of(new Partition(0, 0)));
        when(partitionService.getPartitions(1)).thenReturn(ImmutableList.of(new Partition(1, 0)));
        when(partitionService.getPartitions(2)).thenReturn(ImmutableList.of());

        assertEquals(balancingService.getMovePartitions(), ImmutableMap.of(0, 0, 1, 0, 2, 0));
    }

    @Test
    public void noSignificantDifference() {
        when(diskService.getDisks()).thenReturn(ImmutableList.of(0, 1, 2));
        when(partitionService.getPartitions(0)).thenReturn(ImmutableList.of(new Partition(0, 0), new Partition(0, 1)));
        when(partitionService.getPartitions(1)).thenReturn(ImmutableList.of(new Partition(1, 0)));
        when(partitionService.getPartitions(2)).thenReturn(ImmutableList.of());

        assertEquals(balancingService.getMovePartitions(), ImmutableMap.of(0, 0, 1, 0, 2, 0));
    }

    @Test
    public void balanceSingleDisk() {
        when(diskService.getDisks()).thenReturn(ImmutableList.of(0, 1, 2));
        when(partitionService.getPartitions(0)).thenReturn(ImmutableList.of(new Partition(0, 0), new Partition(0, 1), new Partition(0, 2), new Partition(0, 3)));
        when(partitionService.getPartitions(1)).thenReturn(ImmutableList.of(new Partition(1, 0), new Partition(1, 1)));
        when(partitionService.getPartitions(2)).thenReturn(ImmutableList.of());

        assertEquals(balancingService.getMovePartitions(), ImmutableMap.of(0, 1, 1, 0, 2, 0));
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
        JChannel jChannel() {
            return mock(JChannel.class);
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

        @Provides
        @Singleton
        FileConfiguration fileConfiguration() {
            return mock(FileConfiguration.class);
        }

        @Override
        protected void configure() {
        }
    }
}
