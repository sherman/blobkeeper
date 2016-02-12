package io.blobkeeper.index.service;

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

import com.google.common.base.Objects;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Range;
import io.blobkeeper.index.dao.IndexDao;
import io.blobkeeper.index.domain.DiskIndexElt;
import io.blobkeeper.index.domain.IndexElt;
import io.blobkeeper.index.domain.Partition;
import io.blobkeeper.index.util.MinMaxConsumer;
import org.jetbrains.annotations.NotNull;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;

import static com.google.common.base.Objects.equal;

@Singleton
public class IndexServiceImpl implements IndexService {
    private final Cache<CacheKey, IndexElt> cache = CacheBuilder.newBuilder()
            .maximumSize(1048576)
            .build();

    @Inject
    private IndexDao indexDao;

    @Override
    public IndexElt getById(long id, int type) {
        /*try {
            return cache.get(
                    new CacheKey(id, type),
                    () -> indexDao.getById(id, type)
            );
        } catch (ExecutionException e) {
            //log.error("Can't get index elt from cache", e);
            throw new IllegalStateException(e);
        } catch (CacheLoader.InvalidCacheLoadException e) {
            return null;
        }*/
        return indexDao.getById(id, type);
    }

    @Override
    public List<IndexElt> getListById(long id) {
        return indexDao.getListById(id);
    }

    @Override
    public void add(@NotNull IndexElt indexElt) {
        indexDao.add(indexElt);
    }

    @Override
    public void delete(@NotNull IndexElt indexElt) {
        indexDao.updateDelete(indexElt.getId(), true);
    }

    @Override
    public void restore(@NotNull IndexElt indexElt) {
        indexDao.updateDelete(indexElt.getId(), false);
    }

    @Override
    public void move(@NotNull IndexElt from, @NotNull DiskIndexElt to) {
        indexDao.move(from ,to);
    }

    @Override
    public List<IndexElt> getListByPartition(@NotNull Partition partition) {
        return indexDao.getListByPartition(partition);
    }

    @Override
    public List<IndexElt> getLiveListByPartition(@NotNull Partition partition) {
        return indexDao.getLiveListByPartition(partition);
    }

    @Override
    public Range<Long> getMinMaxRange(@NotNull Partition partition) {
        List<IndexElt> elts = getListByPartition(partition);

        MinMaxConsumer minMax = elts.stream()
                .collect(MinMaxConsumer::new, MinMaxConsumer::accept, MinMaxConsumer::combine);

        if (minMax.isEmpty()) {
            throw new NoIndexRangeException();
        } else {
            return Range.openClosed(minMax.getMin().getId() - 1, minMax.getMax().getId());
        }
    }

    @Override
    public long getSizeOfDeleted(@NotNull Partition partition) {
        return indexDao.getSizeOfDeleted(partition);
    }

    @Override
    public void clear() {
        indexDao.clear();
    }

    private static class CacheKey {
        final long id;
        final int typeId;

        private CacheKey(long id, int typeId) {
            this.id = id;
            this.typeId = typeId;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (null == object) {
                return false;
            }


            if (!(object instanceof CacheKey)) {
                return false;
            }

            CacheKey o = (CacheKey) object;

            return equal(id, o.id)
                    && equal(typeId, o.typeId);
        }

        /**
         * part_id are globally unique by shard
         */
        @Override
        public int hashCode() {
            return Objects.hashCode(id, typeId);
        }
    }
}
