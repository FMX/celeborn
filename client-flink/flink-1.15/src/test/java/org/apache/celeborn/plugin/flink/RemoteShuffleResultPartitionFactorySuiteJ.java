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

package org.apache.celeborn.plugin.flink;

import static org.mockito.Mockito.mock;

import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.runtime.io.network.buffer.BufferPoolFactory;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.junit.Assert;
import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.protocol.CompressionCodec;

/** Tests for {@link RemoteShuffleResultPartitionFactory}. */
public class RemoteShuffleResultPartitionFactorySuiteJ {

  @Test
  public void testGetBufferCompressor() {
    CelebornConf celebornConf = new CelebornConf();
    for (CompressionCodec compressionCodec : CompressionCodec.values()) {
      RemoteShuffleResultPartitionFactory partitionFactory =
          new RemoteShuffleResultPartitionFactory(
              celebornConf.set(
                  CelebornConf.SHUFFLE_COMPRESSION_CODEC().key(), compressionCodec.name()),
              mock(ResultPartitionManager.class),
              mock(BufferPoolFactory.class),
              1);
      if (CompressionCodec.NONE.equals(compressionCodec)) {
        Assert.assertNull(partitionFactory.getBufferCompressor());
      } else if (CompressionCodec.LZ4.equals(compressionCodec)) {
        Assert.assertNotNull(partitionFactory.getBufferCompressor());
      } else {
        Assert.assertThrows(
            IllegalConfigurationException.class, partitionFactory::getBufferCompressor);
      }
    }
  }
}
