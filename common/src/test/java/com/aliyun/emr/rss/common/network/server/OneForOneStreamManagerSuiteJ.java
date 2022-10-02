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

package com.aliyun.emr.rss.common.network.server;

import io.netty.channel.Channel;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.aliyun.emr.rss.common.meta.LocalShuffleBuffers;

public class OneForOneStreamManagerSuiteJ {
  @Test
  public void streamStatesAreFreedWhenConnectionIsClosedEvenIfBufferIteratorThrowsException() {
    OneForOneStreamManager manager = new OneForOneStreamManager();

    @SuppressWarnings("unchecked")
    LocalShuffleBuffers buffers = Mockito.mock(LocalShuffleBuffers.class);

    @SuppressWarnings("unchecked")
    LocalShuffleBuffers buffers2 = Mockito.mock(LocalShuffleBuffers.class);

    Channel dummyChannel = Mockito.mock(Channel.class, Mockito.RETURNS_SMART_NULLS);
    manager.registerStream(buffers, dummyChannel);
    manager.registerStream(buffers2, dummyChannel);

    Assert.assertEquals(2, manager.numStreamStates());

    manager.connectionTerminated(dummyChannel);
    assert manager.streams.isEmpty();
  }
}
