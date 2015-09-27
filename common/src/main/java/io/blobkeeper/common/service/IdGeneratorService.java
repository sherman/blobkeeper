package io.blobkeeper.common.service;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;

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
 *
 *
 * Based on twitter snowflake idea
 */
@Singleton
public class IdGeneratorService {
    private static final Logger log = LoggerFactory.getLogger(IdGeneratorService.class);

    private static long epoch = 1388534400000L; // 2014

    private long shardIdBits = 10L;
    private long maxShardId = -1L ^ (-1L << shardIdBits); // 1024
    private long sequenceBits = 12L;

    private long shardIdShift = sequenceBits;
    private long timestampLeftShift = sequenceBits + shardIdBits;
    private long maxSequenceId = -1L ^ (-1L << sequenceBits); // 4096

    private long lastTimestamp = -1L;
    private int sequence;

    public synchronized long generate(int shardId) {
        if (shardId > maxShardId || shardId < 0) {
            throw new IllegalArgumentException(format("Shard id can't be greater than %s or less than 0", maxShardId));
        }

        long ts = currentTimeMillis();

        if (ts < lastTimestamp) {
            log.error("Clock is moving backwards. Rejecting requests until {}.", lastTimestamp);
            throw new IllegalStateException(format("Clock moved backwards. Refusing to generate id for %s milliseconds", lastTimestamp - ts));
        }

        if (lastTimestamp == ts || lastTimestamp < 0) {
            sequence++;
        } else {
            sequence = 0;
        }

        // omg, too many photo for one shard!
        if (sequence > maxSequenceId) {
            ts = getNextMills(lastTimestamp);
            sequence = 0;
        }

        lastTimestamp = ts;

        return ((ts - epoch) << timestampLeftShift) |
                (shardId << shardIdShift) |
                sequence;
    }

    public int getShard(long id) {
        int mask = ((1 << shardIdBits) - 1) << shardIdShift;
        return (int) ((id & mask) >> shardIdShift);
    }

    public long getTimestamp(long id) {
        return ((id >> timestampLeftShift) + epoch);
    }

    public HashCode getHash(long id) {
        return Hashing.murmur3_128().hashLong(id);
    }

    private long getNextMills(long lastTimestamp) {
        long ts = currentTimeMillis();
        while (ts <= lastTimestamp) {
            ts = currentTimeMillis();
        }
        return ts;
    }
}
