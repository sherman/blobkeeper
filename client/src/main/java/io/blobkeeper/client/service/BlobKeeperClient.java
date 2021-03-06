package io.blobkeeper.client.service;

/*
 * Copyright (C) 2015-2017 by Denis M. Gabaydulin
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

import com.google.common.util.concurrent.Service;
import io.blobkeeper.common.domain.api.*;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Response;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.Map;

public interface BlobKeeperClient extends Service {
    Response getFile(long id, int type);

    Response getFile(long id, int type, String authToken);

    Response addFile(@NotNull File file, @NotNull Map<String, String> headers);

    Response addFile(long id, int type, @NotNull File file, @NotNull Map<String, String> headers);

    Response deleteFile(long id, @NotNull String apiToken);

    ListenableFuture<Response> deleteFileAsync(long id, @NotNull String apiToken);

    Response isMaster();

    Response setMaster(@NotNull SetMasterApiRequest request);

    Response removeMaster(@NotNull EmptyRequest request);

    Response refreshDisks(@NotNull RefreshDiskRequest request);

    Response repair(@NotNull RepairDiskRequest request);

    Response balance(@NotNull RebalancingDiskRequest request);
}
