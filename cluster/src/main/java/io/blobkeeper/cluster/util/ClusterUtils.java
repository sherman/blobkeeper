package io.blobkeeper.cluster.util;

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

import io.blobkeeper.cluster.domain.MerkleTreeInfo;
import io.blobkeeper.file.service.PartitionService;
import io.blobkeeper.index.domain.Partition;
import org.jetbrains.annotations.NotNull;
import org.jgroups.Address;
import org.jgroups.Header;
import org.jgroups.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.blobkeeper.cluster.domain.CustomMessageHeader.CUSTOM_MESSAGE_HEADER;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

@Singleton
public class ClusterUtils {
    private static final Logger log = LoggerFactory.getLogger(ClusterUtils.class);

    @Inject
    private PartitionService partitionService;

    public Map<Integer, MerkleTreeInfo> getExpectedTrees(int disk, @NotNull List<Partition> partitions) {
        log.info("All partitions {}", partitions);

        Partition active = partitionService.getActivePartition(disk);
        checkNotNull(active, "Active partition is required!");

        // get all partitions with completed merkle tries (readonly)
        Map<Integer, MerkleTreeInfo> expectedData = partitions
                .stream()
                .filter(partition -> partition.getTree() != null)
                .map(
                        partition -> {
                            MerkleTreeInfo merkleTreeInfo = new MerkleTreeInfo();
                            merkleTreeInfo.setDisk(partition.getDisk());
                            merkleTreeInfo.setPartition(partition.getId());
                            merkleTreeInfo.setTree(partition.getTree());

                            return merkleTreeInfo;
                        })
                .collect(toMap(MerkleTreeInfo::getPartition, identity()));

        log.info("Filtered master partitions {}", expectedData);

        return expectedData;
    }

    @NotNull
    public static <T> Message createMessage(
            @NotNull Address src,
            @NotNull Address dst,
            T object,
            Header header
    ) {
        Message message = new Message();
        message.setDest(dst);
        message.setSrc(src);
        message.putHeader(CUSTOM_MESSAGE_HEADER, header);
        message.setObject(object);
        return message;
    }
}
