package io.blobkeeper.server.util;

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

import io.netty.channel.DefaultFileRegion;

import java.nio.channels.FileChannel;

public class UnClosableFileRegion extends DefaultFileRegion {

    /**
     * Create a new instance
     *
     * @param file     the {@link java.nio.channels.FileChannel} which should be transfered
     * @param position the position from which the transfer should start
     * @param count    the number of bytes to transfer
     */
    public UnClosableFileRegion(FileChannel file, long position, long count) {
        super(file, position, count);
    }

    @Override
    protected void deallocate() {
        /*_*/
    }
}
