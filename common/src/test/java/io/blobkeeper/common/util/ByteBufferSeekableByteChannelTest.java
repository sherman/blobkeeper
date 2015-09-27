package io.blobkeeper.common.util;

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

import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

import static org.testng.Assert.assertEquals;

public class ByteBufferSeekableByteChannelTest {
    @Test
    public void size() throws IOException {
        SeekableByteChannel channel = new ByteBufferSeekableByteChannel(ByteBuffer.wrap(new byte[]{0x1, 0x2, 0x3, 0x4}));
        assertEquals(channel.size(), 4);
        channel.close();
    }

    @Test
    public void getPosition() throws IOException {
        SeekableByteChannel channel = new ByteBufferSeekableByteChannel(ByteBuffer.wrap(new byte[]{0x1, 0x2, 0x3, 0x4}));
        assertEquals(channel.position(), 0);
        channel.write(ByteBuffer.wrap(new byte[]{0x01}));
        assertEquals(channel.position(), 1);
        channel.close();
    }

    @Test
    public void setPosition() throws IOException {
        SeekableByteChannel channel = new ByteBufferSeekableByteChannel(ByteBuffer.wrap(new byte[]{0x1, 0x2, 0x3, 0x4}));
        assertEquals(channel.position(), 0);
        channel.position(3);
        assertEquals(channel.position(), 3);
        channel.close();
    }

    @Test(expectedExceptions = IOException.class)
    public void truncate() throws IOException {
        ByteBuffer original = ByteBuffer.wrap(new byte[]{0x1, 0x2, 0x3, 0x4});
        SeekableByteChannel channel = new ByteBufferSeekableByteChannel(original);
        channel.truncate(0);
    }
}
