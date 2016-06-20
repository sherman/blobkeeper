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

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.blobkeeper.cluster.configuration.ClusterPropertiesConfiguration;
import io.blobkeeper.cluster.domain.DifferenceInfo;
import io.blobkeeper.cluster.domain.MerkleTreeInfo;
import io.blobkeeper.cluster.domain.Node;
import io.blobkeeper.cluster.domain.CustomMessageHeader;
import io.blobkeeper.common.logging.MdcContext;
import io.blobkeeper.common.util.LeafNode;
import io.blobkeeper.common.util.MdcUtils;
import io.blobkeeper.common.util.MerkleTree;
import io.blobkeeper.file.domain.File;
import io.blobkeeper.file.domain.ReplicationFile;
import io.blobkeeper.file.service.DiskService;
import io.blobkeeper.file.service.FileListService;
import io.blobkeeper.index.domain.CacheKey;
import io.blobkeeper.index.domain.Partition;
import io.blobkeeper.index.service.IndexCacheService;
import io.blobkeeper.index.service.IndexService;
import io.blobkeeper.index.util.IndexUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jgroups.*;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.blocks.locking.LockService;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;

import static com.google.common.collect.Iterables.toArray;
import static com.jayway.awaitility.Awaitility.await;
import static com.jayway.awaitility.Duration.FIVE_HUNDRED_MILLISECONDS;
import static io.blobkeeper.cluster.domain.CustomMessageHeader.CUSTOM_MESSAGE_HEADER;
import static io.blobkeeper.cluster.domain.Role.MASTER;
import static io.blobkeeper.cluster.domain.Role.SLAVE;
import static io.blobkeeper.common.logging.MdcContext.SRC_NODE;
import static io.blobkeeper.common.util.GuavaCollectors.toImmutableList;
import static io.blobkeeper.common.util.MdcUtils.setCurrentContext;
import static io.blobkeeper.common.util.MerkleTree.MAX_LEVEL;
import static io.blobkeeper.common.util.Utils.createEmptyTree;
import static io.blobkeeper.file.util.FileUtils.buildMerkleTree;
import static java.lang.System.currentTimeMillis;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.runAsync;
import static org.jgroups.blocks.ResponseMode.GET_FIRST;
import static org.jgroups.jmx.JmxConfigurator.registerChannel;
import static org.jgroups.jmx.JmxConfigurator.unregisterChannel;
import static org.jgroups.util.Util.createConcurrentMap;
import static org.jgroups.util.Util.getMBeanServer;

@Singleton
public class ClusterMembershipServiceImpl extends ReceiverAdapter implements ClusterMembershipService {
    private static final Logger log = LoggerFactory.getLogger(ClusterMembershipServiceImpl.class);

    private static final Short SET_MASTER = 0x1;
    private static final Short GET_MASTER = 0x2;
    private static final Short REMOVE_MASTER = 0x4;
    private static final Short GET_NODE = 0x5;
    private static final Short GET_TREE_INFO = 0x6;
    private static final Short GET_TREE_DIFF_NODE = 0x7;
    private static final Short DELETE_PARTITION_FILE = 0x8;

    private static final String CLUSTER_NAME = "blobkeeper_cluster";
    private static final String MASTER_LOCK = "master_lock";

    @Inject
    private FileListService fileListService;

    @Inject
    private LockService lockService;

    @Inject
    private ReplicationHandlerService replicationHandlerService;

    @Inject
    private RepairService repairService;

    @Inject
    private MasterChangedListener listener;

    @Inject
    private ClusterPropertiesConfiguration configuration;

    @Inject
    private ReplicationClientService replicationClient;

    @Inject
    private IndexService indexService;

    @Inject
    private IndexUtils indexUtils;

    @Inject
    private DiskService diskService;

    @Inject
    private IndexCacheService indexCacheService;

    private final Random random = new Random();

    private JChannel channel;
    private volatile Node self;
    private volatile Node master;

    private RpcDispatcher dispatcher = null;

    private final ExecutorService masterSelectorAndRepairExecutor = Executors.newFixedThreadPool(
            16,
            new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("RepairWorker-%d")
                    .build()
    );

    private static final Map<Short, Method> methods = createConcurrentMap(16);

    static {
        try {
            methods.put(SET_MASTER, ClusterMembershipServiceImpl.class.getMethod("_setMaster", Address.class));
            methods.put(GET_MASTER, ClusterMembershipServiceImpl.class.getMethod("_getMaster"));
            methods.put(REMOVE_MASTER, ClusterMembershipServiceImpl.class.getMethod("_removeMaster"));
            methods.put(GET_NODE, ClusterMembershipServiceImpl.class.getMethod("_getNode"));
            methods.put(GET_TREE_INFO, ClusterMembershipServiceImpl.class.getMethod("_getMerkleTreeInfo", int.class, int.class));
            methods.put(GET_TREE_DIFF_NODE, ClusterMembershipServiceImpl.class.getMethod("_getDifference", int.class, int.class));
            methods.put(DELETE_PARTITION_FILE, ClusterMembershipServiceImpl.class.getMethod("_deletePartitionFile", int.class, int.class));
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }

        ClassConfigurator.add(CUSTOM_MESSAGE_HEADER, CustomMessageHeader.class);
    }

    @Override
    public void start(@NotNull String name) {
        try {
            channel = new JChannel(ClusterMembershipServiceImpl.class.getClassLoader().getResourceAsStream(configuration.getClusterConfig()));
            channel.setDiscardOwnMessages(true);
            channel.setName(name);

            dispatcher = new RpcDispatcher(channel, null, this, this);
            dispatcher.setMethodLookup(methods::get);

            dispatcher.setMessageListener(this);

            channel.connect(CLUSTER_NAME);
            channel.getState(null, 10000);

            setCurrentContext(new MdcContext(ImmutableMap.of(SRC_NODE, getSelfNode().toString())));

            log.info("Node is started");

            registerChannel(channel, getMBeanServer(), name);
        } catch (Exception e) {
            log.error("Can't create channel", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {
        await().forever().pollInterval(FIVE_HUNDRED_MILLISECONDS).until(
                () -> {
                    log.trace("Waiting for repair");
                    return !repairService.isRepairInProgress();
                });

        dispatcher.stop();

        try {
            unregisterChannel(getMBeanServer(), channel.getName());
        } catch (Exception e) {
            log.error("Can't unregister channel", e);
        }

        channel.close();

        log.debug("Node is stopped");

        self = null;
        master = null;
    }

    @Override
    public JChannel getChannel() {
        return channel;
    }

    @Override
    public Node getMaster() {
        return master;
    }

    @Override
    public void setMaster(@NotNull Node node) {
        _setMaster(node.getAddress());
    }

    @Override
    public boolean trySetMaster(@NotNull Address newMaster) {
        // TODO: change executor
        List<CompletableFuture<Void>> setters = getNodes().stream()
                .map(node -> runAsync(() -> setMaster(node.getAddress(), newMaster)))
                .collect(toImmutableList());

        allOf(toArray(setters, CompletableFuture.class))
                .join();

        return true;
    }

    @Override
    public Node getSelfNode() {
        if (self == null) {
            setSelfNode(new Node(SLAVE, channel.getAddress(), currentTimeMillis()));
            log.info("Self updated");
        }
        return self;
    }

    @Override
    public List<Node> getNodes() {
        Node master = getMaster();

        // FIXME: find a better way for optimization of getNode()
        return channel.getView().getMembers().stream()
                .map(address -> address.equals(master.getAddress()) ? new Node(MASTER, address, 0L) : new Node(SLAVE, address, 0L))
                .collect(toImmutableList());
    }

    @Override
    public DifferenceInfo getDifference(@NotNull MerkleTreeInfo treeInfo) {
        // get actual merkle tree
        MerkleTreeInfo localTreeInfo = _getMerkleTreeInfo(treeInfo.getDisk(), treeInfo.getPartition());

        List<LeafNode> difference = MerkleTree.difference(treeInfo.getTree(), localTreeInfo.getTree());

        DifferenceInfo differenceInfo = new DifferenceInfo();
        differenceInfo.setDisk(treeInfo.getDisk());
        differenceInfo.setPartition(treeInfo.getPartition());
        differenceInfo.setDifference(difference);

        return differenceInfo;
    }

    @Override
    public boolean isMaster() {
        return getSelfNode().equals(master);
    }

    @Override
    public boolean tryRemoveMaster() {
        // TODO: change executor
        List<CompletableFuture<Void>> removers = getNodes().stream()
                .map(node -> runAsync(() -> removeMaster(node.getAddress())))
                .collect(toImmutableList());

        allOf(toArray(removers, CompletableFuture.class))
                .join();

        return true;
    }

    @Override
    public void deletePartitionFile(int disk, int partition) {
        List<CompletableFuture<Void>> partitionDeleteWorkers = getNodes().stream()
                .map(node -> runAsync(() -> deletePartitionFile(node.getAddress(), disk, partition)))
                .collect(toImmutableList());

        allOf(toArray(partitionDeleteWorkers, CompletableFuture.class))
                .join();
    }

    @Override
    public Optional<Node> getNodeForRepair(boolean active) {
        if (active) {
            return Optional.ofNullable(getMaster());
        } else {
            List<Node> nodes = getNodes();

            return nodes.stream()
                    .filter(node -> !node.equals(getSelfNode()))
                    .skip(nodes.size() > 1 ? random.nextInt(nodes.size() - 1) : 0)
                    .findFirst();

        }
    }

    @Override
    public void setMaster(@NotNull Address node, @NotNull Address newMaster) {
        if (getSelfNode().getAddress().equals(node)) {
            _setMaster(newMaster);
            return;
        }

        try {
            dispatcher.callRemoteMethod(
                    node,
                    new MethodCall(SET_MASTER, newMaster),
                    new RequestOptions(GET_FIRST, 1000L)
            );
        } catch (Exception e) {
            log.error("Can't call method " + SET_MASTER + " on remote node " + node, e);
        }
    }

    @Override
    public Node getMaster(@NotNull Address node) {
        log.debug("Getting master");

        if (getSelfNode().getAddress().equals(node)) {
            return _getMaster();
        }

        try {
            return dispatcher.callRemoteMethod(
                    node,
                    new MethodCall(GET_MASTER),
                    new RequestOptions(ResponseMode.GET_FIRST, 1000L));
        } catch (Exception e) {
            log.error("Can't call method " + GET_MASTER + " on remote node " + node, e);
        }

        return null;
    }

    @Override
    public Node getNode(@NotNull Address node) {
        log.debug("Getting node object");

        if (getSelfNode().getAddress().equals(node)) {
            return _getNode();
        }

        try {
            return dispatcher.callRemoteMethod(
                    node,
                    new MethodCall(GET_NODE),
                    new RequestOptions(ResponseMode.GET_FIRST, 1000L));
        } catch (Exception e) {
            log.error("Can't call method " + GET_NODE + " on remote node " + node, e);
        }

        return null;
    }

    @NotNull
    @Override
    public MerkleTreeInfo getMerkleTreeInfo(@NotNull Address node, int disk, int partition) {
        log.debug("Getting merkle tree info {} for disk {}", partition, disk);

        if (getSelfNode().getAddress().equals(node)) {
            return _getMerkleTreeInfo(disk, partition);
        }

        try {
            return dispatcher.callRemoteMethod(
                    node,
                    new MethodCall(GET_TREE_INFO, disk, partition),
                    new RequestOptions(ResponseMode.GET_FIRST, 5 * 60 * 1000L)); // 5 minutes
        } catch (Exception e) {
            log.error("Can't call method " + GET_TREE_INFO + " on remote node " + node, e);
        }

        MerkleTreeInfo merkleTreeInfo = new MerkleTreeInfo();
        merkleTreeInfo.setDisk(disk);
        merkleTreeInfo.setPartition(partition);
        return merkleTreeInfo;
    }

    @Nullable
    @Override
    public DifferenceInfo getDifference(@NotNull Address node, int disk, int partition) {
        log.debug("Getting merkle tree diff {} for disk {}", partition, disk);

        if (getSelfNode().getAddress().equals(node)) {
            return _getDifference(disk, partition);
        }

        try {
            return dispatcher.callRemoteMethod(
                    node,
                    new MethodCall(GET_TREE_DIFF_NODE, disk, partition),
                    new RequestOptions(ResponseMode.GET_FIRST, 5 * 60 * 1000L)); // 5 minutes
        } catch (Exception e) {
            log.error("Can't call method " + GET_TREE_DIFF_NODE + " on remote node " + node, e);
        }

        return null;
    }

    @Override
    public void removeMaster(@NotNull Address node) {
        if (getSelfNode().getAddress().equals(node)) {
            _removeMaster();
            return;
        }

        try {
            dispatcher.callRemoteMethod(
                    node,
                    new MethodCall(REMOVE_MASTER),
                    new RequestOptions(ResponseMode.GET_NONE, 1000L)
            );
        } catch (Exception e) {
            log.error("Can't call method " + REMOVE_MASTER + " on remote node " + node, e);
        }
    }

    @Override
    public void deletePartitionFile(@NotNull Address node, int disk, int partition) {
        if (getSelfNode().getAddress().equals(node)) {
            _deletePartitionFile(disk, partition);
            return;
        }

        try {
            dispatcher.callRemoteMethod(
                    node,
                    new MethodCall(DELETE_PARTITION_FILE, disk, partition),
                    new RequestOptions(GET_FIRST, 10000L)
            );
        } catch (Exception e) {
            log.error("Can't call method " + DELETE_PARTITION_FILE + " on remote node " + node, e);
        }
    }

    @Override
    public void getState(OutputStream output) throws Exception {
        Util.objectToStream(master, new DataOutputStream(output));
    }

    @Override
    public void setState(InputStream input) throws Exception {
        master = (Node) Util.objectFromStream(new DataInputStream(input));
    }

    @Override
    public void receive(Message message) {
        if (log.isTraceEnabled()) {
            log.trace("Message received:" + message);
        }

        CustomMessageHeader customMessageHeader;
        try {
            customMessageHeader = (CustomMessageHeader) message.getHeader(CUSTOM_MESSAGE_HEADER);
        } catch (ClassCastException e) {
            throw new IllegalArgumentException("Can't find replication header!", e);
        }

        if (null == customMessageHeader) {
            throw new IllegalArgumentException("Can't find replication header!");
        }

        switch (customMessageHeader.getCommand()) {
            case FILE:
                handleReplicatedFile(message);
                break;

            case REPLICATION_REQUEST:
                masterSelectorAndRepairExecutor.submit(new ReplicationRequestHandler(message));
                break;

            case CACHE_INVALIDATE_REQUEST:
                handleCacheInvalidate(message);
                break;

            default:
                throw new IllegalArgumentException(String.format("Do not know what to do with %s", customMessageHeader.getCommand()));
        }
    }

    @Override
    public void viewAccepted(final View view) {
        super.viewAccepted(view);
        log.info("Nodes list is changed {} for node {}, creator {}", view, getSelfNode(), view.getCreator());

        masterSelectorAndRepairExecutor.submit(new RepairTask(view));
    }

    /**
     * RPC methods impl.
     */

    public void _setMaster(Address newMaster) {
        log.info("Set new master {}", newMaster);

        Node oldMaster = master;
        master = new Node(MASTER, newMaster, currentTimeMillis());

        if (getSelfNode().getAddress().equals(newMaster)) {
            setSelfNode(master);
            log.info("Self updated");
        } else {
            setSelfNode(new Node(SLAVE, getSelfNode().getAddress(), currentTimeMillis()));
        }

        listener.onMasterChanged(self, oldMaster, master);
    }

    public Node _getMaster() {
        return master;
    }

    public Node _getNode() {
        return getSelfNode();
    }

    public void _removeMaster() {
        log.info("Remove master");

        Node oldMaster = master;
        master = null;

        setSelfNode(new Node(SLAVE, getSelfNode().getAddress(), currentTimeMillis()));

        listener.onMasterChanged(self, oldMaster, master);
    }

    @NotNull
    public MerkleTreeInfo _getMerkleTreeInfo(int disk, int partition) {
        MerkleTreeInfo merkleTreeInfo = new MerkleTreeInfo();
        merkleTreeInfo.setDisk(disk);
        merkleTreeInfo.setPartition(partition);

        Partition partitionObject = new Partition(disk, partition);

        File file = null;
        try {
            file = fileListService.getFile(disk, partition);
            if (null == file) {
                merkleTreeInfo.setTree(createEmptyTree(indexService.getMinMaxRange(partitionObject), MAX_LEVEL));
                return merkleTreeInfo;
            } else {
                merkleTreeInfo.setTree(buildMerkleTree(indexService, file, partitionObject));
                return merkleTreeInfo;
            }
        } finally {
            if (null != file) {
                try {
                    file.close();
                } catch (Exception ignored) {
                }
            }
        }
    }

    public DifferenceInfo _getDifference(int disk, int partition) {
        MerkleTree expectedTree = indexUtils.buildMerkleTree(new Partition(disk, partition));

        MerkleTreeInfo expected = new MerkleTreeInfo();
        expected.setDisk(disk);
        expected.setPartition(partition);
        expected.setTree(expectedTree);

        return getDifference(expected);
    }

    public void _deletePartitionFile(int disk, int partition) {
        log.info("Delete partition file: {} {}", disk, partition);

        diskService.deleteFile(new Partition(disk, partition));
    }

    private void setSelfNode(Node node) {
        self = node;
        setCurrentContext(new MdcContext(ImmutableMap.of(SRC_NODE, getSelfNode().toString())));
    }

    private void handleNodeChanging() {
        lockService.setChannel(channel);

        boolean acquired = false;
        Lock lock = lockService.getLock(MASTER_LOCK);
        try {
            acquired = lock.tryLock(5000L, TimeUnit.MILLISECONDS);
            if (!acquired) {
                return;
            }

            if (configuration.isMaster()) {
                log.info("Master comes from configuration");

                if (null != getMaster() && !getSelfNode().equals(getMaster())) {
                    throw new IllegalStateException("Master already exists!");
                }

                setMaster(getSelfNode());
            }

            log.trace("Locked {}");

            View view = channel.getView();

            Node currentMaster = getMaster();
            log.info("Current master is {}", currentMaster);

            boolean masterIsAvailable = isCurrentMasterAvailable(view, currentMaster);

            // just added slave
            if (masterIsAvailable) {
                // event could be handle on any node
                // TODO: optimize, send master only to the added node
                trySetMaster(currentMaster.getAddress());
            } else {
                ofNullable(master).ifPresent(
                        master -> {
                            // remove master on current node
                            log.warn("Master {} is not available", master);
                            this.removeMaster(getSelfNode().getAddress());
                        }
                );
            }
        } catch (Exception e) {
            log.error("Can't select master", e);
        } finally {
            if (acquired) {
                lock.unlock();
                log.trace("Unlocked");
            }
        }
    }

    private boolean isCurrentMasterAvailable(View view, Node currentMaster) {
        return null != currentMaster && view.getMembers().contains(currentMaster.getAddress());
    }

    private void handleReplicatedFile(Message message) {
        try {
            Object file = message.getObject();
            if (file instanceof ReplicationFile) {
                replicationHandlerService.handleReplicated((ReplicationFile) message.getObject());
            } else {
                log.error("Do not know what to do with {}", file.getClass());
            }
        } catch (Exception e) {
            log.error("Can't replicate block", e);
        }
    }

    private void handleCacheInvalidate(Message message) {
        try {
            Object cacheKey = message.getObject();
            if (cacheKey instanceof CacheKey) {
                indexCacheService.remove((CacheKey) cacheKey);
            }
        } catch (Exception e) {
            log.error("Can't invalidate cache", e);
        }
    }

    private class RepairTask implements Runnable {
        private final View view;

        private RepairTask(View view) {
            this.view = view;
        }

        @Override
        public void run() {
            try {
                setCurrentContext(new MdcContext(ImmutableMap.of(SRC_NODE, getSelfNode().toString())));

                handleNodeChanging();

                log.info("Current view is {}", getChannel().getView());

                if (null == getMaster()) {
                    log.info("There is no master, skip repairing");
                    return;
                }

                if (isRepairRequired()) {
                    log.info("Repairing started");
                    repairService.repair(false);
                }
            } catch (Throwable e) {
                log.error("Can't select master", e);
            } finally {
                MdcUtils.clearCurrentContext();
            }
        }

        private boolean isRepairRequired() {
            return view.getMembers().size() > 1;
        }
    }

    private class ReplicationRequestHandler implements Runnable {
        private final Message message;

        public ReplicationRequestHandler(Message message) {
            this.message = message;
        }

        @Override
        public void run() {
            try {
                setCurrentContext(new MdcContext(ImmutableMap.of(SRC_NODE, getSelfNode().toString())));

                replicationClient.replicate((DifferenceInfo) message.getObject(), message.getSrc());
            } catch (Exception e) {
                log.error("Can't replicate file {}", message.getObject(), e);
            } finally {
                MdcUtils.clearCurrentContext();
            }
        }
    }
}
