package io.blobkeeper.file.util;

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
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Optional.ofNullable;

@Singleton
public class DiskStatistic {
    private static final Logger log = LoggerFactory.getLogger(DiskStatistic.class);

    private static final String DISK_PART_CREATED_NAME_PATTERN = "blobkeeper.disk.%d.partitions.created";
    private static final String DISK_IS_FULL_PATTERN = "blobkeeper.disk.%d.is.full";

    @Inject
    private MetricRegistry metricRegistry;

    public void onCreatePartition(int disk) {
        String metricName = getDiskPartCreatedMetric(disk);
        Counter counter = metricRegistry.getCounters().get(metricName);
        if (counter == null) {
            counter = metricRegistry.register(metricName, new Counter());
        }
        counter.inc();
    }

    public void onDiskIsFullError(int disk) {
            String metricName = getDiskIsFullMetric(disk);
            Counter counter = metricRegistry.getCounters().get(metricName);
            if (counter == null) {
                counter = metricRegistry.register(metricName, new Counter());
            }
            counter.inc();
        }

    public long getCreatedPartitions(int disk) {
        return ofNullable(metricRegistry.getCounters().get(getDiskPartCreatedMetric(disk)))
                .map(Counter::getCount)
                .orElse(0L);

    }

    private String getDiskPartCreatedMetric(int disk) {
        return format(DISK_PART_CREATED_NAME_PATTERN, disk);
    }

    private String getDiskIsFullMetric(int disk) {
        return format(DISK_IS_FULL_PATTERN, disk);
    }
}
