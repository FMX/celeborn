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

package org.apache.celeborn.common.meta;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.roaringbitmap.RoaringBitmap;

public class ReduceFileMeta implements FileMeta {
  private final AtomicBoolean sorted = new AtomicBoolean(false);
  private final List<Long> chunkOffsets;
  private RoaringBitmap mapIds;
  private long chunkSize;
  private long nextBoundary;

  public ReduceFileMeta(long chunkSize) {
    this.chunkOffsets = new ArrayList<>();
    chunkOffsets.add(0L);
    this.chunkSize = chunkSize;
    nextBoundary = chunkSize;
  }

  public ReduceFileMeta(List<Long> chunkOffsets, long chunkSize) {
    this.chunkOffsets = chunkOffsets;
    this.chunkSize = chunkSize;
    nextBoundary = chunkSize;
  }

  public synchronized List<Long> getChunkOffsets() {
    return chunkOffsets;
  }

  public synchronized void addChunkOffset(long offset) {
    nextBoundary = offset + chunkSize;
    chunkOffsets.add(offset);
  }

  public void updateChunkOffset(long bytesFlushed, boolean force) {
    if (bytesFlushed >= nextBoundary || force) {
      addChunkOffset(bytesFlushed);
      nextBoundary = bytesFlushed + chunkSize;
    }
  }

  public synchronized long getLastChunkOffset() {
    return chunkOffsets.get(chunkOffsets.size() - 1);
  }

  public synchronized int getNumChunks() {
    if (chunkOffsets.isEmpty()) {
      return 0;
    } else {
      return chunkOffsets.size() - 1;
    }
  }

  public void setSorted() {
    synchronized (sorted) {
      sorted.set(true);
    }
  }

  public AtomicBoolean getSorted() {
    return sorted;
  }

  public void setMapIds(RoaringBitmap mapIds) {
    this.mapIds = mapIds;
  }

  public void removeMapIds(int startIndex, int endIndex) {
    for (int i = startIndex; i < endIndex; i++) {
      mapIds.remove(i);
    }
  }

  public RoaringBitmap getMapIds() {
    return mapIds;
  }

  public long getChunkSize() {
    return chunkSize;
  }
}
