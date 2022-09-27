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

package com.aliyun.emr.rss.service.deploy.worker.storage;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.emr.rss.common.RssConf;
import com.aliyun.emr.rss.common.exception.AlreadyClosedException;
import com.aliyun.emr.rss.common.meta.DiskStatus;
import com.aliyun.emr.rss.common.meta.FileInfo;
import com.aliyun.emr.rss.common.metrics.source.AbstractSource;
import com.aliyun.emr.rss.common.network.server.MemoryTracker;
import com.aliyun.emr.rss.common.protocol.PartitionSplitMode;
import com.aliyun.emr.rss.common.protocol.PartitionType;
import com.aliyun.emr.rss.common.protocol.StorageInfo;
import com.aliyun.emr.rss.common.unsafe.Platform;
import com.aliyun.emr.rss.service.deploy.worker.WorkerSource;

/*
 * Note: Once FlushNotifier.exception is set, the whole file is not available.
 *       That's fine some of the internal state(e.g. bytesFlushed) may be inaccurate.
 */
public final class FileWriter implements DeviceObserver {
  private static final Logger logger = LoggerFactory.getLogger(FileWriter.class);
  private static final long WAIT_INTERVAL_MS = 20;

  private final FileInfo fileInfo;
  private FileChannel channel;
  private FSDataOutputStream stream;
  private volatile boolean closed;

  private final AtomicInteger numPendingWrites = new AtomicInteger();
  private long nextBoundary;
  private long bytesFlushed;

  public final Flush flusher;
  private final int flushWorkerIndex;
  private CompositeByteBuf flushBuffer;

  private final long chunkSize;
  private final long timeoutMs;

  private final long flushBufferSize;

  private final DeviceMonitor deviceMonitor;
  private final AbstractSource source; // metrics

  private long splitThreshold = 0;
  private final PartitionSplitMode splitMode;
  private final PartitionType partitionType;
  private final boolean rangeReadFilter;

  private Runnable destroyHook;
  private boolean deleted = false;
  private RoaringBitmap mapIdBitMap = null;

  @Override
  public void notifyError(String mountPoint, DiskStatus diskStatus) {
    if (!notifier.hasException()) {
      notifier.setException(
          new IOException("Device ERROR! Disk: " + mountPoint + " : " + diskStatus));
    }
    deviceMonitor.unregisterFileWriter(this);
  }

  private final FlushNotifier notifier = new FlushNotifier();

  public FileWriter(
      FileInfo fileInfo,
      Flush flusher,
      AbstractSource workerSource,
      RssConf rssConf,
      DeviceMonitor deviceMonitor,
      long splitThreshold,
      PartitionSplitMode splitMode,
      PartitionType partitionType,
      boolean rangeReadFilter)
      throws IOException {
    this.fileInfo = fileInfo;
    this.flusher = flusher;
    this.flushWorkerIndex = flusher.getWorkerIndex();
    this.chunkSize = RssConf.chunkSize(rssConf);
    this.nextBoundary = this.chunkSize;
    this.timeoutMs = RssConf.fileWriterTimeoutMs(rssConf);
    this.splitThreshold = splitThreshold;
    this.flushBufferSize = RssConf.workerFlushBufferSize(rssConf);
    this.deviceMonitor = deviceMonitor;
    this.splitMode = splitMode;
    this.partitionType = partitionType;
    this.rangeReadFilter = rangeReadFilter;
    if (fileInfo.isHdfs()) {
      stream = StorageManager.hdfsFs().create(fileInfo.getHdfsPath(), true);
    }
    if (fileInfo.isLocal()) {
      channel = new FileOutputStream(fileInfo.getFilePath()).getChannel();
    }
    source = workerSource;
    logger.debug("FileWriter {} split threshold {} mode {}", this, splitThreshold, splitMode);
    if (rangeReadFilter) {
      this.mapIdBitMap = new RoaringBitmap();
    }
    takeBuffer();
  }

  public FileInfo getFileInfo() {
    return fileInfo;
  }

  public File getFile() {
    return fileInfo.getFile();
  }

  public void incrementPendingWrites() {
    numPendingWrites.incrementAndGet();
  }

  public void decrementPendingWrites() {
    numPendingWrites.decrementAndGet();
  }

  private void flush(boolean finalFlush) throws IOException {
    int numBytes = flushBuffer.readableBytes();
    notifier.checkException();
    notifier.numPendingFlushes.incrementAndGet();
    FlushTask task = null;
    if (fileInfo.isLocal()) {
      task = new LocalFlushTask(flushBuffer, channel, notifier);
    }
    if (fileInfo.isHdfs()) {
      task = new HdfsFlushTask(flushBuffer, stream, notifier);
    }
    if (fileInfo.isLocal() || fileInfo.isHdfs()) {
      addTask(task);
      flushBuffer = null;
    }
    bytesFlushed += numBytes;
    maybeSetChunkOffsets(finalFlush);
  }

  private void maybeSetChunkOffsets(boolean forceSet) {
    if (bytesFlushed >= nextBoundary || forceSet) {
      fileInfo.addChunkOffset(bytesFlushed);
      nextBoundary = bytesFlushed + chunkSize;
    }
  }

  private boolean isChunkOffsetValid() {
    // Consider a scenario where some bytes have been flushed
    // but the chunk offset boundary has not yet been updated.
    // we should check if the chunk offset boundary equals
    // bytesFlush or not. For example:
    // The last record is a giant record and it has been flushed
    // but its size is smaller than the nextBoundary, then the
    // chunk offset will not be set after flushing. we should
    // set it during FileWriter close.
    return fileInfo.getLastChunkOffset() == bytesFlushed;
  }

  /**
   * assume data size is less than chunk capacity
   *
   * @param data
   */
  public void write(ByteBuf data) throws IOException {
    if (closed) {
      String msg = "FileWriter has already closed!, fileName " + fileInfo.getFilePath();
      logger.warn(msg);
      throw new AlreadyClosedException(msg);
    }

    if (notifier.hasException()) {
      return;
    }

    int mapId = 0;
    if (rangeReadFilter) {
      byte[] header = new byte[16];
      data.markReaderIndex();
      data.readBytes(header);
      data.resetReaderIndex();
      mapId = Platform.getInt(header, Platform.BYTE_ARRAY_OFFSET);
    }

    final int numBytes = data.readableBytes();
    MemoryTracker.instance().incrementDiskBuffer(numBytes);
    synchronized (this) {
      if (rangeReadFilter) {
        mapIdBitMap.add(mapId);
      }
      if (fileInfo.isMemory()) {
        MemoryTracker.instance().recordMemoryShuffle(numBytes);
      }
      if (flushBuffer.readableBytes() != 0
          && flushBuffer.readableBytes() + numBytes >= this.flushBufferSize) {
        flush(false);
        takeBuffer();
      }
    }

    data.retain();
    flushBuffer.addComponent(true, data);
    numPendingWrites.decrementAndGet();
  }

  public RoaringBitmap getMapIdBitMap() {
    return mapIdBitMap;
  }

  public StorageInfo getStorageInfo() {
    if (flusher instanceof LocalBaseFlusher) {
      LocalBaseFlusher localFlusher = (LocalBaseFlusher) flusher;
      return new StorageInfo(localFlusher.diskType(), localFlusher.mountPoint(), true);
    } else if (flusher instanceof MemoryFlusher) {
      return new StorageInfo(StorageInfo.Type.MEMORY, true);
    } else {
      if (deleted) {
        return null;
      } else {
        return new StorageInfo(StorageInfo.Type.HDFS, true, fileInfo.getFilePath());
      }
    }
  }

  public long close() throws IOException {
    if (closed) {
      String msg = "FileWriter has already closed! fileName " + fileInfo.getFilePath();
      logger.error(msg);
      throw new AlreadyClosedException(msg);
    }

    try {
      waitOnNoPending(numPendingWrites);
      closed = true;

      synchronized (this) {
        if (flushBuffer.readableBytes() > 0) {
          flush(true);
        }
        if (!isChunkOffsetValid()) {
          maybeSetChunkOffsets(true);
        }
      }

      waitOnNoPending(notifier.numPendingFlushes);
    } finally {
      if (!fileInfo.isMemory()) {
        returnBuffer();
      }
      try {
        if (fileInfo.isLocal()) {
          channel.close();
        }
        if (fileInfo.isHdfs()) {
          closeHdfs();
        }
      } catch (IOException e) {
        logger.warn("close file writer " + this + " failed", e);
      }

      // unregister from DeviceMonitor
      deviceMonitor.unregisterFileWriter(this);
    }
    return bytesFlushed;
  }

  private void closeHdfs() throws IOException {
    if (stream != null) {
      stream.close();
      if (StorageManager.hdfsFs().exists(fileInfo.getHdfsPeerWriterSuccessPath())) {
        StorageManager.hdfsFs().delete(fileInfo.getHdfsPath(), false);
        deleted = true;
      } else {
        StorageManager.hdfsFs().create(fileInfo.getHdfsWriterSuccessPath()).close();
        FSDataOutputStream indexOutputStream =
            StorageManager.hdfsFs().create(fileInfo.getHdfsIndexPath());
        indexOutputStream.writeInt(fileInfo.getChunkOffsets().size());
        for (Long offset : fileInfo.getChunkOffsets()) {
          indexOutputStream.writeLong(offset);
        }
        indexOutputStream.close();
      }
    }
  }

  public void destroy() {
    if (!closed) {
      closed = true;
      notifier.setException(new IOException("destroyed"));
      returnBuffer();
      try {
        if (fileInfo.isLocal()) {
          destroyLocal();
        }
        if (fileInfo.isHdfs()) {
          destroyHdfs();
        }
        if (fileInfo.isMemory()) {
          destroyMemory();
        }
      } catch (IOException e) {
        logger.warn(
            "Close channel failed for file {} caused by {}.",
            fileInfo.getFilePath(),
            e.getMessage());
      }
    }

    try {
      fileInfo.deleteAllFiles(StorageManager.hdfsFs());
    } catch (Exception e) {
      logger.warn("clean hdfs file {}", fileInfo.getFilePath());
    }

    // unregister from DeviceMonitor
    deviceMonitor.unregisterFileWriter(this);
    destroyHook.run();
  }

  private void destroyLocal() throws IOException {
    if (channel != null) {
      channel.close();
    }
  }

  private void destroyHdfs() throws IOException {
    if (stream != null) {
      stream.close();
    }
  }

  private void destroyMemory() {
    returnBuffer();
  }

  public void registerDestroyHook(List<FileWriter> fileWriters) {
    FileWriter thisFileWriter = this;
    destroyHook =
        () -> {
          synchronized (fileWriters) {
            fileWriters.remove(thisFileWriter);
          }
        };
  }

  public IOException getException() {
    if (notifier.hasException()) {
      return notifier.exception.get();
    } else {
      return null;
    }
  }

  private void waitOnNoPending(AtomicInteger counter) throws IOException {
    long waitTime = timeoutMs;
    while (counter.get() > 0 && waitTime > 0) {
      try {
        notifier.checkException();
        TimeUnit.MILLISECONDS.sleep(WAIT_INTERVAL_MS);
      } catch (InterruptedException e) {
        IOException ioe = new IOException(e);
        notifier.setException(ioe);
        throw ioe;
      }
      waitTime -= WAIT_INTERVAL_MS;
    }
    if (counter.get() > 0) {
      IOException ioe = new IOException("Wait pending actions timeout.");
      notifier.setException(ioe);
      throw ioe;
    }
    notifier.checkException();
  }

  private void takeBuffer() {
    // metrics start
    String metricsName = null;
    String fileAbsPath = null;
    if (source.samplePerfCritical()) {
      metricsName = WorkerSource.TakeBufferTime();
      fileAbsPath = fileInfo.getFilePath();
      source.startTimer(metricsName, fileAbsPath);
    }

    // real action
    flushBuffer = flusher.takeBuffer(flushBuffer);

    // metrics end
    if (source.samplePerfCritical()) {
      source.stopTimer(metricsName, fileAbsPath);
    }

    if (flushBuffer == null) {
      IOException e =
          new IOException(
              "Take buffer encounter error from BaseFlusher: " + flusher.bufferQueueInfo());
      notifier.setException(e);
    }
  }

  private void addTask(FlushTask task) throws IOException {
    if (!flusher.addTask(task, timeoutMs, flushWorkerIndex)) {
      IOException e = new IOException("Add flush task timeout.");
      notifier.setException(e);
      throw e;
    }
  }

  private synchronized void returnBuffer() {
    if (flushBuffer != null) {
      flusher.returnBuffer(flushBuffer);
      flushBuffer = null;
    }
  }

  public int hashCode() {
    return fileInfo.getFilePath().hashCode();
  }

  public boolean equals(Object obj) {
    return (obj instanceof FileWriter)
        && fileInfo.getFilePath().equals(((FileWriter) obj).fileInfo.getFilePath());
  }

  public String toString() {
    return fileInfo.getFilePath();
  }

  public void flushOnMemoryPressure() throws IOException {
    synchronized (this) {
      if (flushBuffer != null && flushBuffer.readableBytes() != 0) {
        flush(false);
        takeBuffer();
      }
    }
  }

  public long getSplitThreshold() {
    return splitThreshold;
  }

  public PartitionSplitMode getSplitMode() {
    return splitMode;
  }

  // These empty methods are intended to match scala 2.11 restrictions that
  // trait can not be used as an interface with default implementation.
  @Override
  public void notifyHealthy(String mountPoint) {}

  @Override
  public void notifyHighDiskUsage(String mountPoint) {}

  public long getBytesFlushed() {
    return bytesFlushed;
  }
}
