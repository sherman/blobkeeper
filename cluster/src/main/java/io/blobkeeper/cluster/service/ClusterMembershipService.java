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

import com.google.inject.ImplementedBy;
import io.blobkeeper.cluster.domain.DifferenceInfo;
import io.blobkeeper.cluster.domain.MerkleTreeInfo;
import io.blobkeeper.cluster.domain.Node;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jgroups.Address;
import org.jgroups.JChannel;

import java.util.List;
import java.util.Optional;

@ImplementedBy(ClusterMembershipServiceImpl.class)
public interface ClusterMembershipService {
    void start(@NotNull String name);

    void stop();

    JChannel getChannel();

    /**
     * @return master node (must be the same on any cluster node)
     */
    Node getMaster();

    void setMaster(@NotNull Node node);

    /**
     * Sets a new master in the cluster
     */
    boolean trySetMaster(@NotNull Address newMaster);

    Node getSelfNode();

    List<Node> getNodes();

    DifferenceInfo getDifference(@NotNull MerkleTreeInfo treeInfo);

    boolean isMaster();

    /**
     * Removes a master in the cluster
     */
    boolean tryRemoveMaster();

    void deletePartitionFile(int disk, int partition);

    Optional<Node> getNodeForRepair(boolean active);

    /**
     * RPC method to set {@param newMaster} on the given {@param node}
     */
    void setMaster(@NotNull Address node, @NotNull Address newMaster);

    /**
     * RPC method to get master node at the given {@param node}
     */
    Node getMaster(@NotNull Address node);

    /**
     * RPC method to get node at the given {@param node}
     */
    Node getNode(@NotNull Address node);

    /**
     * RPC method to retrieve actual merkle tree about {@param partition} at the given {@param node}
     */
    @NotNull
    MerkleTreeInfo getMerkleTreeInfo(@NotNull Address node, int disk, int partition);

    /**
     * RPC method to retrieve actual merkle tree diff given {@param node} and expected
     */
    @Nullable
    DifferenceInfo getDifference(@NotNull Address node, int disk, int partition);

    /**
     * RPC method to remove master on the given {@param node}
     */
    void removeMaster(@NotNull Address node);

    /**
     * RPC method to remove master on the given {@param node}
     */
    void deletePartitionFile(@NotNull Address node, int disk, int partition);
}
