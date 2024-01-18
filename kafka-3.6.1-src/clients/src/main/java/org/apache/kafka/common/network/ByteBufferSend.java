/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.network;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A send backed by an array of byte buffers
 */
public class ByteBufferSend implements Send {
    // How much data should be written in total
    private final long size;
    // ByteBuffers to write data to channel
    protected final ByteBuffer[] buffers;
    // How much data is left to write
    private long remaining;
    private boolean pending = false;

    public ByteBufferSend(ByteBuffer... buffers) {
        this.buffers = buffers;
        for (ByteBuffer buffer : buffers)
            remaining += buffer.remaining();
        // compute sum of bytes to write
        this.size = remaining;
    }

    public ByteBufferSend(ByteBuffer[] buffers, long size) {
        this.buffers = buffers;
        this.size = size;
        this.remaining = size;
    }

    @Override
    public boolean completed() {
        return remaining <= 0 && !pending;
    }

    @Override
    public long size() {
        return this.size;
    }

    @Override
    public long writeTo(TransferableChannel channel) throws IOException {
        // 1. write all buffers
        long written = channel.write(buffers);
        if (written < 0)
            throw new EOFException("Wrote negative bytes to channel. This shouldn't happen.");
        // 2. Writing once may not necessarily be able to write all the data, so we should compute "how much data is left to write"
        remaining -= written;
        pending = channel.hasPendingWrites();
        return written;
    }

    public long remaining() {
        return remaining;
    }

    @Override
    public String toString() {
        return "ByteBufferSend(" +
            ", size=" + size +
            ", remaining=" + remaining +
            ", pending=" + pending +
            ')';
    }
    //  Encapsulate the buffer to be sent, [ByteBuffer size] + [ByteBuffer buffer]
    public static ByteBufferSend sizePrefixed(ByteBuffer buffer) {
        // 1. 4 byte sizeBuffer to restore request
        ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
        // 2. write request size ( Int -> 4byte -> putInt )
        sizeBuffer.putInt(0, buffer.remaining());
        // 3. metadata buffer + actual data buffer
        return new ByteBufferSend(sizeBuffer, buffer);
    }
}
