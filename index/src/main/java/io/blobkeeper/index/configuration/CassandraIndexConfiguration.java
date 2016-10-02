package io.blobkeeper.index.configuration;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryOptions;
import com.google.inject.Provides;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import static com.datastax.driver.core.ConsistencyLevel.SERIAL;
import static com.google.common.base.Splitter.on;

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

@Singleton
public class CassandraIndexConfiguration {

    @Inject
    @Named("blobkeeper.cassandra.nodes")
    private String nodes;

    @Inject
    @Named("blobkeeper.cassandra.keyspace")
    private String keyspace;

    @Inject
    @Named("blobkeeper.cassandra.consistency.level")
    private ConsistencyLevel consistencyLevel;

    @Provides
    @Singleton
    public Cluster createCluster() {
        Cluster.Builder builder = Cluster
                .builder()
                .withQueryOptions(
                        new QueryOptions()
                                .setConsistencyLevel(consistencyLevel)
                                .setSerialConsistencyLevel(SERIAL)
                )
                .withClusterName("blobkeeper-cluster");

        for (String node : getNodes()) {
            builder.addContactPoint(node);
        }

        return builder.build();
    }

    public String getKeyspace() {
        return keyspace;
    }

    public ConsistencyLevel getConsistencyLevel() {
            return consistencyLevel;
        }

    private Iterable<String> getNodes() {
        return on(",").split(nodes);
    }
}
