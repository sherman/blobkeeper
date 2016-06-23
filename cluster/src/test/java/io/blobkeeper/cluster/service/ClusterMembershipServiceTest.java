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
import com.google.common.collect.ImmutableSet;
import com.google.inject.*;
import io.blobkeeper.cluster.domain.Node;
import io.blobkeeper.common.service.*;
import io.blobkeeper.common.service.SecondServerRootModule;
import io.blobkeeper.file.configuration.FileModule;
import io.blobkeeper.file.service.FileStorage;
import io.blobkeeper.index.dao.IndexDao;
import io.blobkeeper.index.dao.PartitionDao;
import junit.framework.Assert;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.inject.*;
import javax.inject.Singleton;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.jayway.awaitility.Awaitility.await;
import static com.jayway.awaitility.Duration.ONE_HUNDRED_MILLISECONDS;
import static io.blobkeeper.cluster.domain.Role.MASTER;
import static io.blobkeeper.cluster.domain.Role.SLAVE;
import static junit.framework.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class ClusterMembershipServiceTest extends BaseMultipleInjectorTest {

    private ClusterMembershipService membershipService1;

    private ClusterMembershipService membershipService2;

    private FileStorage fileStorage1;

    private FileStorage fileStorage2;

    private CountedMasterChangedListener listener1;

    private CountedMasterChangedListener listener2;

    @Test
    public void startEmpty() {
        membershipService1.start("node1");

        assertEquals(membershipService1.getSelfNode().getRole(), SLAVE);

        membershipService1.stop();
    }

    @Test
    public void keepOriginalMasterIfExists() throws InterruptedException {
        membershipService1.start("node1");

        assertFalse(membershipService1.isMaster());
        membershipService1.setMaster(membershipService1.getSelfNode());

        membershipService2.start("node2");

        await().forever().pollInterval(ONE_HUNDRED_MILLISECONDS).until(
                () -> listener1.getMasterChangedCount() >= 3 && listener2.getMasterChangedCount() >= 1
        );

        assertEquals(membershipService1.getSelfNode().getRole(), MASTER);
        assertEquals(membershipService2.getSelfNode().getRole(), SLAVE);
        assertEquals(membershipService1.getMaster(), membershipService2.getMaster());
        assertTrue(membershipService1.isMaster());
        assertFalse(membershipService2.isMaster());
        verifyZeroInteractions(fileStorage1);

        // stop slave
        membershipService2.stop();

        await().forever().pollInterval(ONE_HUNDRED_MILLISECONDS).until(
                () -> listener1.getMasterChangedCount() >= 2
        );

        // keep original master
        assertEquals(membershipService1.getSelfNode().getRole(), MASTER);
        assertTrue(membershipService1.isMaster());
        verifyZeroInteractions(fileStorage1);

        membershipService1.stop();
    }

    @Test
    public void removeMasterOnSalveIfMasterIsDisconnected() {
        membershipService1.start("node1");

        assertFalse(membershipService1.isMaster());
        membershipService1.setMaster(membershipService1.getSelfNode());

        membershipService2.start("node2");

        await().forever().pollInterval(ONE_HUNDRED_MILLISECONDS).until(
                () -> listener1.getMasterChangedCount() >= 3 && listener2.getMasterChangedCount() >= 1
        );

        assertEquals(membershipService1.getSelfNode().getRole(), MASTER);
        assertEquals(membershipService2.getSelfNode().getRole(), SLAVE);
        assertEquals(membershipService1.getMaster(), membershipService2.getMaster());
        assertTrue(membershipService1.isMaster());
        assertFalse(membershipService2.isMaster());
        verifyZeroInteractions(fileStorage1);

        // stop master
        membershipService1.stop();

        await().forever().pollInterval(ONE_HUNDRED_MILLISECONDS).until(
                () -> listener2.getMasterChangedCount() >= 3
        );

        // the master is not available on the slave node
        assertEquals(membershipService2.getSelfNode().getRole(), SLAVE);
        assertFalse(membershipService2.getMaster().isPresent());
        verifyZeroInteractions(fileStorage1);

        membershipService2.stop();
    }

    @Test
    public void getNodes() throws InterruptedException {
        membershipService1.start("node1");

        assertFalse(membershipService1.isMaster());
        membershipService1.setMaster(membershipService1.getSelfNode());

        membershipService2.start("node2");

        await().forever().pollInterval(ONE_HUNDRED_MILLISECONDS).until(
                () -> listener1.getMasterChangedCount() >= 3 && listener2.getMasterChangedCount() >= 1
        );

        assertEquals(membershipService1.getNodes(), ImmutableList.of(membershipService1.getSelfNode(), membershipService2.getSelfNode()));
        assertEquals(membershipService2.getNodes(), ImmutableList.of(membershipService1.getSelfNode(), membershipService2.getSelfNode()));

        membershipService2.stop();
        membershipService1.stop();
    }

    @Test
    public void notEnoughNodesForRepair() {
        membershipService1.start("node1");
        membershipService1.setMaster(membershipService1.getSelfNode());

        assertFalse(membershipService1.getNodeForRepair(false).isPresent());
        assertFalse(membershipService1.getNodeForRepair(true).isPresent());

        membershipService1.stop();
    }

    @Test
    public void notEnoughNodesForRepairForRepairActive() {
        membershipService1.start("node1");
        membershipService2.start("node2");

        assertEquals(membershipService1.getNodeForRepair(false).get().getAddress().toString(), "node2");
        assertFalse(membershipService1.getNodeForRepair(true).isPresent());
        assertEquals(membershipService2.getNodeForRepair(false).get().getAddress().toString(), "node1");
        assertFalse(membershipService2.getNodeForRepair(true).isPresent());

        membershipService1.stop();
        membershipService2.stop();
    }

    @Test
    public void activeRepairOnlyFromMaster() {
        membershipService1.start("node1");
        membershipService2.start("node2");
        membershipService1.trySetMaster(membershipService1.getSelfNode().getAddress());

        assertEquals(membershipService1.getNodeForRepair(false).get().getAddress().toString(), "node2");
        assertFalse(membershipService1.getNodeForRepair(true).isPresent());
        assertEquals(membershipService2.getNodeForRepair(false).get().getAddress().toString(), "node1");
        assertEquals(membershipService2.getNodeForRepair(true).get().getAddress().toString(), "node1");

        membershipService1.stop();
        membershipService2.stop();
    }

    @BeforeMethod(dependsOnMethods = "createInjectors")
    protected void setUp() {
        membershipService1 = firstServerInjector.getInstance(ClusterMembershipService.class);
        listener1 = (CountedMasterChangedListener) firstServerInjector.getInstance(MasterChangedListener.class);
        fileStorage1 = firstServerInjector.getInstance(FileStorage.class);

        membershipService2 = secondServerInjector.getInstance(ClusterMembershipService.class);
        listener2 = (CountedMasterChangedListener) secondServerInjector.getInstance(MasterChangedListener.class);
        fileStorage2 = secondServerInjector.getInstance(FileStorage.class);
    }

    @Override
    protected Set<Module> getFirstInjectorModules() {
        return ImmutableSet.of(new FirstInjectorMocks(), new FirstServerRootModule(), new FileModule());
    }

    @Override
    protected Set<Module> getSecondInjectorModules() {
        return ImmutableSet.of(new SecondInjectorMocks(), new SecondServerRootModule(), new FileModule());
    }

    @Override
    protected Set<Module> getThirdInjectorModules() {
        return ImmutableSet.of(new ThirdInjectorMocks(), new ThirdServerRootModule(), new FileModule());
    }

    public static class FirstInjectorMocks extends AbstractModule {
        @Provides
        @Singleton
        PartitionDao partitionDao() {
            return Mockito.mock(PartitionDao.class);
        }

        @Provides
        @Singleton
        IndexDao indexDao() {
            return Mockito.mock(IndexDao.class);
        }

        @Provides
        @Singleton
        MasterChangedListener changedListener(Injector injector) {
            DefaultMasterChangedListener listener = new DefaultMasterChangedListener();
            injector.injectMembers(listener);
            return new CountedMasterChangedListener(listener);
        }

        @Provides
        @Singleton
        FileStorage fileStorage() {
            return mock(FileStorage.class);
        }

        @Override
        protected void configure() {
        }
    }

    public static class SecondInjectorMocks extends AbstractModule {
        @Provides
        @Singleton
        PartitionDao partitionDao() {
            return Mockito.mock(PartitionDao.class);
        }

        @Provides
        @Singleton
        IndexDao indexDao() {
            return Mockito.mock(IndexDao.class);
        }

        @Provides
        @Singleton
        MasterChangedListener changedListener(Injector injector) {
            DefaultMasterChangedListener listener = new DefaultMasterChangedListener();
            injector.injectMembers(listener);
            return new CountedMasterChangedListener(listener);
        }

        @Provides
        @Singleton
        FileStorage fileStorage() {
            return mock(FileStorage.class);
        }

        @Override
        protected void configure() {
        }
    }

    public static class ThirdInjectorMocks extends AbstractModule {
        @Provides
        @Singleton
        PartitionDao partitionDao() {
            return Mockito.mock(PartitionDao.class);
        }

        @Provides
        @Singleton
        IndexDao indexDao() {
            return Mockito.mock(IndexDao.class);
        }

        @Provides
        @Singleton
        MasterChangedListener changedListener(Injector injector) {
            DefaultMasterChangedListener listener = new DefaultMasterChangedListener();
            injector.injectMembers(listener);
            return new CountedMasterChangedListener(listener);
        }

        @Provides
        @Singleton
        FileStorage fileStorage() {
            return mock(FileStorage.class);
        }

        @Override
        protected void configure() {
        }
    }

    public static class CountedMasterChangedListener implements MasterChangedListener {
        private static final Logger log = LoggerFactory.getLogger(CountedMasterChangedListener.class);

        private final AtomicInteger counter;
        private final DefaultMasterChangedListener originalListener;

        public CountedMasterChangedListener(DefaultMasterChangedListener originalListener) {
            counter = new AtomicInteger();
            this.originalListener = originalListener;
        }

        @Override
        public void onMasterChanged(@NotNull Node selfNode, @Nullable Node oldMaster, @Nullable Node newMaster) {
            originalListener.onMasterChanged(selfNode, oldMaster, newMaster);

            counter.incrementAndGet();
        }

        public int getMasterChangedCount() {
            return counter.get();
        }
    }
}
