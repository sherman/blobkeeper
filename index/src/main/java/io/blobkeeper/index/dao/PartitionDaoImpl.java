package io.blobkeeper.index.dao;

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

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import io.blobkeeper.common.util.GuavaCollectors;
import io.blobkeeper.common.util.MerkleTree;
import io.blobkeeper.common.util.SerializationUtils;
import io.blobkeeper.index.configuration.CassandraIndexConfiguration;
import io.blobkeeper.index.domain.Partition;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.nio.ByteBuffer;
import java.util.List;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static java.nio.ByteBuffer.wrap;
import static java.util.stream.StreamSupport.stream;

@Singleton
public class PartitionDaoImpl implements PartitionDao {
    private static final Logger log = LoggerFactory.getLogger(PartitionDaoImpl.class);

    private final Session session;
    private final PreparedStatement insertQuery;
    private final PreparedStatement selectLastQuery;
    private final PreparedStatement selectByIdQuery;
    private final PreparedStatement truncateQuery;
    private final PreparedStatement updateCrcQuery;
    private final PreparedStatement updateTreeQuery;
    private final PreparedStatement selectByDiskQuery;

    @Inject
    public PartitionDaoImpl(CassandraIndexConfiguration configuration) {
        session = configuration.createCluster().connect(configuration.getKeyspace());

        insertQuery = session.prepare(
                insertInto("BlobPartition")
                        .value("disk", bindMarker())
                        .value("part", bindMarker())
                        .value("crc", bindMarker())
        );

        selectLastQuery = session.prepare(
                select().all()
                        .from("BlobPartition")
                        .where(eq("disk", bindMarker()))
                        .orderBy(desc("part"))
                        .limit(1)
        );

        truncateQuery = session.prepare(truncate("BlobPartition"));

        updateCrcQuery = session.prepare(
                update("BlobPartition")
                        .with(set("crc", bindMarker()))
                        .where(eq("disk", bindMarker()))
                        .and(eq("part", bindMarker()))
        );

        updateTreeQuery = session.prepare(
                update("BlobPartition")
                        .with(set("tree", bindMarker()))
                        .where(eq("disk", bindMarker()))
                        .and(eq("part", bindMarker()))
        );

        selectByDiskQuery = session.prepare(
                select().all()
                        .from("BlobPartition")
                        .where(eq("disk", bindMarker()))
                        .orderBy(desc("part"))
        );

        selectByIdQuery = session.prepare(
                select().all()
                        .from("BlobPartition")
                        .where(eq("disk", bindMarker()))
                        .and(eq("part", bindMarker()))
        );
    }

    @Override
    public Partition getLastPartition(int disk) {
        ResultSet result = session.execute(selectLastQuery.bind(disk));
        if (result.getAvailableWithoutFetching() > 1) {
            throw new IllegalStateException("Too many rows");
        }

        return stream(result.spliterator(), false)
                .map(this::mapRow)
                .findFirst()
                .orElse(null);
    }

    @Override
    public void add(@NotNull Partition partition) {
        session.execute(insertQuery.bind(partition.getDisk(), partition.getId(), partition.getCrc()));
    }

    @Override
    public void updateCrc(@NotNull Partition partition) {
        session.execute(updateCrcQuery.bind(partition.getCrc(), partition.getDisk(), partition.getId()));
    }

    @NotNull
    @Override
    public List<Partition> getPartitions(int disk) {
        ResultSet result = session.execute(selectByDiskQuery.bind(disk));

        return stream(result.spliterator(), false)
                .map(this::mapRow)
                .collect(GuavaCollectors.toImmutableList());

    }

    @Override
    public Partition getById(int disk, int id) {
        ResultSet result = session.execute(selectByIdQuery.bind(disk, id));

        if (result.getAvailableWithoutFetching() > 1) {
            throw new IllegalStateException("Too many rows");
        }

        return stream(result.spliterator(), false)
                .map(this::mapRow)
                .findFirst()
                .orElse(null);
    }

    @Override
    public void updateTree(@NotNull Partition partition, @NotNull MerkleTree tree) {
        session.execute(updateTreeQuery.bind(
                        wrap(SerializationUtils.serialize(tree)),
                        partition.getDisk(),
                        partition.getId())
        );
    }

    @Override
    public void clear() {
        session.execute(truncateQuery.bind());
    }

    private Partition mapRow(Row row) {
        Partition partition = new Partition(row.getInt("disk"), row.getInt("part"));
        partition.setCrc(row.getLong("crc"));

        ByteBuffer treeBuffer = row.getBytes("tree");
        if (null != treeBuffer) {
            byte[] treeBufferBytes = new byte[treeBuffer.remaining()];
            treeBuffer.get(treeBufferBytes);

            partition.setTree((MerkleTree) SerializationUtils.deserialize(treeBufferBytes));

            // TODO: ensure merkle tree has been built
        }

        return partition;
    }
}
