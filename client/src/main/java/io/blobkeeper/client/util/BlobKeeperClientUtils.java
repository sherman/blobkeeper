package io.blobkeeper.client.util;

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

import com.jayway.awaitility.Duration;
import io.blobkeeper.client.service.BlobKeeperClient;
import org.jetbrains.annotations.NotNull;

import javax.inject.Singleton;

import static com.jayway.awaitility.Awaitility.await;

@Singleton
public class BlobKeeperClientUtils {

    public void waitForMaster(@NotNull BlobKeeperClient client) {
        await().forever().pollInterval(Duration.FIVE_HUNDRED_MILLISECONDS).until(
                () -> client.isMaster().getResponseBody().contains("true")
        );
    }
}
