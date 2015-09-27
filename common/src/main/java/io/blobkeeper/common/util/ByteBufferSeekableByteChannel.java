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
 *
 *
 * This is an updated version with enhancements made by Daniel Migowski,
 * Andre Bogus, and David Koelle.
 */

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

public class ByteBufferSeekableByteChannel implements SeekableByteChannel {

    private ByteBuffer buffer;
    private boolean open;
    private int size;

    public ByteBufferSeekableByteChannel(ByteBuffer buffer) {
        this.buffer = buffer;
        this.open = true;
        this.size = buffer.capacity();
    }

    public boolean isOpen() {
        return open;
    }

    public void close() throws IOException {
        open = false;
    }

    public int read(ByteBuffer dst) throws IOException {
        if (!buffer.hasRemaining()) {
            return -1;
        }

        int toRead = Math.min(buffer.remaining(), dst.remaining());
        dst.put(read(buffer, toRead));
        return toRead;
    }

    public int write(ByteBuffer src) throws IOException {
        int toWrite = Math.min(buffer.remaining(), src.remaining());
        buffer.put(read(src, toWrite));
        return toWrite;
    }

    public long position() throws IOException {
        return buffer.position();
    }

    public SeekableByteChannel position(long newPosition) throws IOException {
        buffer.position((int) newPosition);
        return this;
    }

    public long size() throws IOException {
        return size;
    }

    public SeekableByteChannel truncate(long size) throws IOException {
        throw new IOException("Truncate operation doesn't support!");
    }

    private ByteBuffer read(ByteBuffer buffer, int count) {
        ByteBuffer slice = buffer.duplicate();
        int limit = buffer.position() + count;
        slice.limit(limit);
        buffer.position(limit);
        return slice;
    }
}
