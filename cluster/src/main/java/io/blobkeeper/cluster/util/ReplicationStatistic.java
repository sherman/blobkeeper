package io.blobkeeper.cluster.util;

/*
 * Copyright (C) 2016 by Denis M. Gabaydulin
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

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class ReplicationStatistic {
    private static final Logger log = LoggerFactory.getLogger(ReplicationStatistic.class);

    private static final String REPLICATION_ELEMENTS = "blobkeeper.replication.elements";

    @Inject
    private MetricRegistry metricRegistry;

    public void init() {
        metricRegistry.register(REPLICATION_ELEMENTS, new Counter());
    }

    public void onReplicationElt() {
        metricRegistry.getCounters().get(REPLICATION_ELEMENTS).inc();
    }
}
