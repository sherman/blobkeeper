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

import com.google.inject.ImplementedBy;
import io.blobkeeper.index.domain.DiskIndexElt;
import io.blobkeeper.index.domain.IndexElt;
import io.blobkeeper.index.domain.Partition;
import org.jetbrains.annotations.NotNull;

import java.util.List;

@ImplementedBy(IndexDaoImpl.class)
public interface IndexDao {
    void add(@NotNull IndexElt elt);

    IndexElt getById(long id, int type);

    List<IndexElt> getListById(long id);

    List<IndexElt> getListByPartition(@NotNull Partition partition);

    void updateDelete(long id, boolean deleted);

    void clear();

    List<IndexElt> getLiveListByPartition(@NotNull Partition partition);

    long getSizeOfDeleted(@NotNull Partition partition);

    void move(@NotNull IndexElt from, @NotNull DiskIndexElt to);
}
