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

import com.datastax.driver.core.*;
import io.blobkeeper.common.util.SerializationUtils;
import io.blobkeeper.index.configuration.CassandraIndexConfiguration;
import io.blobkeeper.index.configuration.IndexConfiguration;
import io.blobkeeper.index.domain.DiskIndexElt;
import io.blobkeeper.index.domain.IndexElt;
import io.blobkeeper.index.domain.Partition;
import org.jetbrains.annotations.NotNull;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static io.blobkeeper.common.util.GuavaCollectors.toImmutableList;
import static io.blobkeeper.common.util.SerializationUtils.serialize;
import static java.nio.ByteBuffer.wrap;
import static java.util.stream.StreamSupport.stream;
import static org.joda.time.DateTimeZone.UTC;

@Singleton
public class IndexDaoImpl implements IndexDao {
    private static final Logger log = LoggerFactory.getLogger(IndexDaoImpl.class);

    private final Session session;

    private final PreparedStatement insertBlobIndexQuery;
    private final PreparedStatement insertBlobIndexByPartQuery;
    private final PreparedStatement getByIdAndTypeQuery;
    private final PreparedStatement getIdsByPartQuery;
    private final PreparedStatement getByIdsQuery;
    private final PreparedStatement updateDeletedQuery;
    private final PreparedStatement getByIdQuery;
    private final PreparedStatement truncateBlobIndexQuery;
    private final PreparedStatement truncateBlobIndexByPartQuery;
    private final PreparedStatement deleteBlobIndexByParQuery;

    @Inject
    private PartitionDao partitionDao;

    @Inject
    private IndexConfiguration indexConfiguration;

    @Inject
    public IndexDaoImpl(CassandraIndexConfiguration configuration) {
        session = configuration.createCluster().connect(configuration.getKeyspace());

        insertBlobIndexQuery = session.prepare(
                insertInto("BlobIndex")
                        .value("id", bindMarker())
                        .value("type", bindMarker())
                        .value("disk", bindMarker())
                        .value("part", bindMarker())
                        .value("created", bindMarker())
                        .value("updated", bindMarker())
                        .value("deleted", bindMarker())
                        .value("crc", bindMarker())
                        .value("offset", bindMarker())
                        .value("length", bindMarker())
                        .value("data", bindMarker())
        );

        insertBlobIndexByPartQuery = session.prepare(
                insertInto("BlobIndexByPart")
                        .value("id", bindMarker())
                        .value("type", bindMarker())
                        .value("disk", bindMarker())
                        .value("part", bindMarker())
        );

        getByIdAndTypeQuery = session.prepare(
                select().all()
                        .from("BlobIndex")
                        .where(eq("id", bindMarker()))
                        .and(eq("type", bindMarker()))
        );

        getIdsByPartQuery = session.prepare(
                select().column("id")
                        .from("BlobIndexByPart")
                        .where(eq("disk", bindMarker()))
                        .and(eq("part", bindMarker()))
        );

        getByIdsQuery = session.prepare(
                select().all()
                        .from("BlobIndex")
                        .where(in("id", bindMarker()))
        );

        updateDeletedQuery = session.prepare(
                update("BlobIndex")
                        .with(set("deleted", bindMarker()))
                        .and(set("updated", bindMarker()))
                        .where(eq("id", bindMarker()))
                        .and(eq("type", bindMarker()))
        );

        getByIdQuery = session.prepare(
                select().all()
                        .from("BlobIndex")
                        .where(eq("id", bindMarker()))
        );

        truncateBlobIndexQuery = session.prepare(truncate("BlobIndex"));
        truncateBlobIndexByPartQuery = session.prepare(truncate("BlobIndexByPart"));

        deleteBlobIndexByParQuery = session.prepare(
                delete().all()
                        .from("BlobIndexByPart")
                        .where(eq("disk", bindMarker()))
                        .and(eq("part", bindMarker()))
                        .and(eq("id", bindMarker()))
                        .and(eq("type", bindMarker()))
        );
    }

    @Override
    public void add(@NotNull IndexElt elt) {
        BatchStatement batchStatement = new BatchStatement();
        batchStatement.add(
                insertBlobIndexQuery.bind(
                        elt.getId(),
                        elt.getType(),
                        elt.getPartition().getDisk(),
                        elt.getPartition().getId(),
                        elt.getCreated(),
                        elt.getUpdated(),
                        elt.isDeleted(),
                        elt.getCrc(),
                        elt.getOffset(),
                        elt.getLength(),
                        wrap(serialize(elt.getMetadata()))
                )
        );
        batchStatement.add(
                insertBlobIndexByPartQuery.bind(
                        elt.getId(),
                        elt.getType(),
                        elt.getPartition().getDisk(),
                        elt.getPartition().getId()
                )
        );

        session.execute(batchStatement);
    }

    @Override
    public IndexElt getById(long id, int type) {
        ResultSet result = session.execute(getByIdAndTypeQuery.bind(id, type));
        if (result.getAvailableWithoutFetching() > 1) {
            throw new IllegalStateException("Too many rows found for key:" + id);
        }

        return stream(result.spliterator(), false)
                .map(this::mapRow)
                .findFirst()
                .orElse(null);
    }

    @Override
    public List<IndexElt> getListById(long id) {
        ResultSet result = session.execute(getByIdQuery.bind(id));

        return stream(result.spliterator(), false)
                .map(this::mapRow)
                .collect(toImmutableList());
    }

    @Override
    public List<IndexElt> getListByPartition(@NotNull Partition partition) {
        return getListByPartition(partition, elt -> true);
    }

    @Override
    public void updateDelete(long id, boolean deleted) {
        updateDelete(id, deleted, DateTime.now(UTC));
    }

    @Override
    public void updateDelete(long id, boolean deleted, @NotNull DateTime updated) {
        List<IndexElt> allTypes = getListById(id);

        List<ResultSetFuture> futures = allTypes
                .stream()
                .map(type -> session.executeAsync(
                        updateDeletedQuery.bind(
                                deleted,
                                updated.getMillis(),
                                type.getId(),
                                type.getType())
                        )
                )
                .collect(toImmutableList());

        futures.forEach(ResultSetFuture::getUninterruptibly);
    }

    @Override
    public void clear() {
        session.execute(truncateBlobIndexQuery.bind());
        session.execute(truncateBlobIndexByPartQuery.bind());
        partitionDao.clear();
    }

    @Override
    public List<IndexElt> getLiveListByPartition(@NotNull Partition partition) {
        Predicate<IndexElt> liveEltsPredicate = isNotDeleted.or(isDeleted.and(new ExpiredPredicate(indexConfiguration.getGcGraceTime())).negate());
        return getListByPartition(partition, liveEltsPredicate);
    }

    @Override
    public long getSizeOfDeleted(@NotNull Partition partition) {
        Predicate<IndexElt> deleteAndExpiredEltsPredicate = isDeleted.and(new ExpiredPredicate(indexConfiguration.getGcGraceTime()));

        return getListByPartition(partition, deleteAndExpiredEltsPredicate).stream()
                .mapToLong(IndexElt::getLength)
                .sum();
    }

    @Override
    public void move(@NotNull IndexElt from, @NotNull DiskIndexElt to) {
        BatchStatement batchStatement = new BatchStatement();
        batchStatement.add(
                insertBlobIndexQuery.bind(
                        from.getId(),
                        from.getType(),
                        to.getPartition().getDisk(),
                        to.getPartition().getId(),
                        from.getCreated(),
                        from.getUpdated(),
                        from.isDeleted(),
                        from.getCrc(),
                        to.getOffset(),
                        to.getLength(),
                        wrap(serialize(from.getMetadata()))
                )
        );
        batchStatement.add(
                deleteBlobIndexByParQuery.bind(
                        from.getPartition().getDisk(),
                        from.getPartition().getId(),
                        from.getId(),
                        from.getType()
                )
        );
        batchStatement.add(
                insertBlobIndexByPartQuery.bind(
                        from.getId(),
                        from.getType(),
                        to.getPartition().getDisk(),
                        to.getPartition().getId()
                )
        );

        session.execute(batchStatement);
    }

    private IndexElt mapRow(Row row) {
        ByteBuffer metadataBuffer = row.getBytes("data");
        byte[] metadataBytes = new byte[metadataBuffer.remaining()];
        metadataBuffer.get(metadataBytes);

        Partition partition = new Partition(row.getInt("disk"), row.getInt("part"));

        return new IndexElt.IndexEltBuilder()
                .id(row.getLong("id"))
                .type(row.getInt("type"))
                .partition(partition)
                .crc(row.getLong("crc"))
                .offset(row.getLong("offset"))
                .length(row.getLong("length"))
                .metadata((Map<String, Object>) SerializationUtils.deserialize(metadataBytes))
                .created(row.getLong("created"))
                .updated(row.getLong("updated"))
                .deleted(row.getBool("deleted"))
                .build();
    }

    private List<IndexElt> getListByPartition(Partition partition, Predicate<IndexElt> predicates) {
        ResultSet result = session.execute(getIdsByPartQuery.bind(partition.getDisk(), partition.getId()));

        List<Long> ids = stream(result.spliterator(), false)
                .map(row -> row.getLong("id"))
                .distinct()
                .collect(toImmutableList());

        result = session.execute(getByIdsQuery.bind(ids));

        return stream(result.spliterator(), false)
                .map(this::mapRow)
                .filter(elt -> partition.equals(elt.getPartition()))
                .filter(predicates)
                .collect(toImmutableList());
    }

    private static Predicate<IndexElt> isDeleted = IndexElt::isDeleted;
    private static Predicate<IndexElt> isNotDeleted = isDeleted.negate();

    private static class ExpiredPredicate implements Predicate<IndexElt> {
        private final int gcGraceTime;
        private final long now;

        public ExpiredPredicate(int gcGraceTime) {
            this.gcGraceTime = gcGraceTime;
            this.now = DateTime.now(UTC).getMillis();
        }

        @Override
        public boolean test(IndexElt elt) {
            return elt.getUpdated() + gcGraceTime * 1000 < now;
        }
    }
}
