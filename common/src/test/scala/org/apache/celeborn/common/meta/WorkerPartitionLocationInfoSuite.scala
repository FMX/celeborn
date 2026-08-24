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

package org.apache.celeborn.common.meta

import java.util

import scala.collection.JavaConverters._

import org.junit.Assert.assertEquals

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.protocol.PartitionLocation

class WorkerPartitionLocationInfoSuite extends CelebornFunSuite {

  test("CELEBORN-575: test after remove the partition location info is empty.") {
    val shuffleKey = "app_12345_12345_1-0"
    val partitionLocation00 = mockPartition(0, 0)
    val partitionLocation01 = mockPartition(0, 1)
    val partitionLocation02 = mockPartition(0, 2)
    val partitionLocation12 = mockPartition(1, 2)
    val partitionLocation11 = mockPartition(1, 1)

    val primaryLocations = new util.ArrayList[PartitionLocation]()
    primaryLocations.add(partitionLocation00)
    primaryLocations.add(partitionLocation01)
    primaryLocations.add(partitionLocation02)
    primaryLocations.add(partitionLocation11)
    primaryLocations.add(partitionLocation12)

    val replicaLocations = new util.ArrayList[PartitionLocation]()
    val partitionLocationReplica00 = mockPartition(0, 0)
    val partitionLocationReplica10 = mockPartition(1, 0)
    replicaLocations.add(partitionLocationReplica00)
    replicaLocations.add(partitionLocationReplica10)

    val workerPartitionLocationInfo = new WorkerPartitionLocationInfo
    workerPartitionLocationInfo.addPrimaryPartitions(shuffleKey, primaryLocations)
    workerPartitionLocationInfo.addReplicaPartitions(shuffleKey, replicaLocations)

    // test remove
    workerPartitionLocationInfo.removePrimaryPartitions(
      shuffleKey,
      primaryLocations.asScala.map(_.getUniqueId).asJava)
    workerPartitionLocationInfo.removeReplicaPartitions(
      shuffleKey,
      replicaLocations.asScala.map(_.getUniqueId).asJava)

    assertEquals(workerPartitionLocationInfo.isEmpty, true)
  }

  test("invalid unique ids do not interrupt lookup or release") {
    val shuffleKey = "app_12345_12345_1-1"
    val location0 = mockPartition(0, 0)
    val location1 = mockPartition(1, 0)
    val location2 = mockPartition(2, 0)
    val locations = util.Arrays.asList(location0, location1, location2)
    val workerPartitionLocationInfo = new WorkerPartitionLocationInfo
    workerPartitionLocationInfo.addPrimaryPartitions(shuffleKey, locations)

    assert(workerPartitionLocationInfo.getPrimaryLocation(shuffleKey, "invalid") == null)
    val lookupResult =
      workerPartitionLocationInfo.getPrimaryLocations(shuffleKey, Array("0-0", "invalid", "2-0"))
    assert(lookupResult(0)._2 eq location0)
    assert(lookupResult(1)._2 == null)
    assert(lookupResult(2)._2 eq location2)

    val releaseResult = workerPartitionLocationInfo.removePrimaryPartitions(
      shuffleKey,
      util.Arrays.asList("0-0", "invalid", "1-0"))
    assertEquals(releaseResult._2, 2)
    assert(workerPartitionLocationInfo.getPrimaryLocation(shuffleKey, "0-0") == null)
    assert(workerPartitionLocationInfo.getPrimaryLocation(shuffleKey, "1-0") == null)
    assert(workerPartitionLocationInfo.getPrimaryLocation(shuffleKey, "2-0") eq location2)

    workerPartitionLocationInfo.removePrimaryPartitions(
      shuffleKey,
      util.Collections.singletonList("2-0"))
    assert(workerPartitionLocationInfo.isEmpty)
  }

  private def mockPartition(partitionId: Int, epoch: Int): PartitionLocation = {
    new PartitionLocation(
      partitionId,
      epoch,
      "mock",
      -1,
      -1,
      -1,
      -1,
      PartitionLocation.Mode.PRIMARY)
  }
}
