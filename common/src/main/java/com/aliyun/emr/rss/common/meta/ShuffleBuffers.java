/*
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

package com.aliyun.emr.rss.common.meta;

import java.util.BitSet;
import java.util.List;

import com.aliyun.emr.rss.common.network.buffer.ManagedBuffer;
import com.aliyun.emr.rss.common.network.util.TransportConf;

public abstract class ShuffleBuffers {
    protected long[] offsets;
    protected int numChunks;
    protected BitSet chunkTracker;
    TransportConf conf;
    protected volatile boolean fullyRead = false;

    public void initlizeByFileInfo(FileInfo fileInfo) {
        numChunks = fileInfo.numChunks();
        if (numChunks > 0) {
            offsets = new long[numChunks + 1];
            List<Long> chunkOffsets = fileInfo.getChunkOffsets();
            for (int i = 0; i <= numChunks; i++) {
                offsets[i] = chunkOffsets.get(i);
            }
        } else {
            offsets = new long[1];
            offsets[0] = 0;
        }
        chunkTracker = new BitSet(numChunks);
        chunkTracker.clear();

    }

    protected abstract ManagedBuffer wrapBuffer(long offset, long len);

    public ManagedBuffer chunk(int chunkIndex, int offset, int len) {
        synchronized (chunkTracker) {
            chunkTracker.set(chunkIndex, true);
        }
        // offset of the beginning of the chunk in the file
        final long chunkOffset = offsets[chunkIndex];
        final long chunkLength = offsets[chunkIndex + 1] - chunkOffset;
        assert offset < chunkLength;
        long length = Math.min(chunkLength - offset, len);
        if (len + offset >= chunkLength) {
            synchronized (chunkTracker) {
                chunkTracker.set(chunkIndex);
            }
            if (chunkIndex == numChunks - 1) {
                fullyRead = true;
            }
        }
        return wrapBuffer(chunkOffset + offset,len);
    }

    public int numChunks() {
        return numChunks;
    }

    public boolean hasAlreadyRead(int chunkIndex) {
        synchronized (chunkTracker) {
            return chunkTracker.get(chunkIndex);
        }
    }

    public boolean isFullyRead() {
        return fullyRead;
    }
}
