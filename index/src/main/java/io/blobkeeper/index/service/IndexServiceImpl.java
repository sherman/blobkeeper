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

import com.google.common.collect.Range;
import io.blobkeeper.index.configuration.IndexConfiguration;
import io.blobkeeper.index.dao.IndexDao;
import io.blobkeeper.index.domain.*;
import io.blobkeeper.index.util.MinMaxConsumer;
import org.jetbrains.annotations.NotNull;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;

import static java.util.Optional.ofNullable;

@Singleton
public class IndexServiceImpl implements IndexService {
    @Inject
    private IndexDao indexDao;

    @Inject
    private IndexCacheService indexCacheService;

    @Inject
    private IndexConfiguration indexConfiguration;

    @Override
    public IndexElt getById(long id, int type) {
        if (indexConfiguration.isCacheEnabled()) {
            return ofNullable(indexCacheService.getById(new CacheKey(id, type)))
                    .orElseGet(
                            () -> {
                                IndexElt elt = indexDao.getById(id, type);

                                if (elt != null) {
                                    indexCacheService.set(elt);
                                }

                                return elt;
                            }

                    );
        } else {
            return indexDao.getById(id, type);
        }
    }

    @NotNull
    @Override
    public List<IndexElt> getListById(long id) {
        return indexDao.getListById(id);
    }

    @Override
    public void add(@NotNull IndexElt indexElt) {
        indexDao.add(indexElt);
    }

    @Override
    public void add(@NotNull IndexTempElt indexElt) {
        indexDao.add(indexElt);
    }

    @Override
    public void delete(@NotNull IndexElt indexElt) {
        try {
            indexDao.updateDelete(indexElt.getId(), true);
        } finally {
            if (indexConfiguration.isCacheEnabled()) {
                indexCacheService.remove(indexElt.toCacheKey());
            }
        }
    }

    @Override
    public void delete(@NotNull IndexTempElt indexElt) {
        indexDao.delete(indexElt);
    }

    @Override
    public void restore(@NotNull IndexElt indexElt) {
        try {
            indexDao.updateDelete(indexElt.getId(), false);
        } finally {
            if (indexConfiguration.isCacheEnabled()) {
                indexCacheService.remove(indexElt.toCacheKey());
            }
        }
    }

    @Override
    public void move(@NotNull IndexElt from, @NotNull DiskIndexElt to) {
        indexDao.move(from, to);
        // TODO: drop cache here?
    }

    @NotNull
    @Override
    public List<IndexElt> getListByPartition(@NotNull Partition partition) {
        return indexDao.getListByPartition(partition);
    }

    @NotNull
    @Override
    public List<IndexElt> getLiveListByPartition(@NotNull Partition partition) {
        return indexDao.getLiveListByPartition(partition);
    }

    @NotNull
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

    @NotNull
    @Override
    public List<IndexTempElt> getTempIndexList(int limit) {
        return indexDao.getTempIndexList(limit);
    }

    @Override
    public void clear() {
        indexDao.clear();
        indexCacheService.clear();
    }
}
