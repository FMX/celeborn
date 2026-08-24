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

package org.apache.celeborn.common.protocol;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.junit.Ignore;
import org.junit.Test;

@Ignore("Manual JOL benchmark; run main to print retained-size comparisons.")
public class PartitionLocationMemorySuiteJ {

  private static final int ENDPOINT_COUNT = 2000;

  public static void main(String[] args) throws Exception {
    int pairCount = args.length > 0 && "largePeer".equals(args[0]) ? 1_000_000 : 10_000;
    new PartitionLocationMemorySuiteJ().printPeerPairFootprint(pairCount, ENDPOINT_COUNT);
  }

  @Test
  public void printPartitionLocationFootprint() throws Exception {
    printPeerPairFootprint(10_000, ENDPOINT_COUNT);
  }

  private void printPeerPairFootprint(int pairCount, int endpointCount) throws Exception {
    compare(
        loadGraphLayout(),
        pairCount + " partition peer pairs across " + endpointCount + " endpoints",
        newOldLocationPairArray(pairCount, endpointCount),
        newLocationPairArray(pairCount, endpointCount));
  }

  private PartitionLocation[] newLocationPairArray(int size, int endpointCount) {
    PartitionLocation[] locations = new PartitionLocation[size * 2];
    for (int i = 0; i < size; i++) {
      int endpointIndex = i % endpointCount;
      PartitionLocation primary = newLocation(i, endpointIndex, PartitionLocation.Mode.PRIMARY);
      PartitionLocation replica = newLocation(i, endpointIndex, PartitionLocation.Mode.REPLICA);
      primary.setPeer(replica);
      replica.setPeer(primary);
      locations[i * 2] = primary;
      locations[i * 2 + 1] = replica;
    }
    return locations;
  }

  private PartitionLocation newLocation(int id, int endpointIndex, PartitionLocation.Mode mode) {
    return new PartitionLocation(
        id,
        0,
        "localhost-" + endpointIndex,
        1001 + endpointIndex,
        1002 + endpointIndex,
        1003 + endpointIndex,
        1004 + endpointIndex,
        mode);
  }

  private PartitionLocationOld[] newOldLocationPairArray(int size, int endpointCount) {
    PartitionLocationOld[] locations = new PartitionLocationOld[size * 2];
    for (int i = 0; i < size; i++) {
      int endpointIndex = i % endpointCount;
      PartitionLocationOld primary =
          newOldLocation(i, endpointIndex, PartitionLocation.Mode.PRIMARY);
      PartitionLocationOld replica =
          newOldLocation(i, endpointIndex, PartitionLocation.Mode.REPLICA);
      primary.setPeer(replica);
      replica.setPeer(primary);
      locations[i * 2] = primary;
      locations[i * 2 + 1] = replica;
    }
    return locations;
  }

  private PartitionLocationOld newOldLocation(
      int id, int endpointIndex, PartitionLocation.Mode mode) {
    return new PartitionLocationOld(
        id,
        0,
        "localhost-" + endpointIndex,
        1001 + endpointIndex,
        1002 + endpointIndex,
        1003 + endpointIndex,
        1004 + endpointIndex,
        mode);
  }

  private Class<?> loadGraphLayout() throws ClassNotFoundException {
    return Class.forName("org.openjdk.jol.info.GraphLayout");
  }

  private void compare(Class<?> graphLayout, String label, Object oldValue, Object newValue)
      throws Exception {
    long oldSize = totalSize(graphLayout, oldValue);
    long newLocationSize = totalSize(graphLayout, newValue);
    long newSizeIncludingInterner = totalSize(graphLayout, newValue, endpointInterner());
    long internerOverhead = newSizeIncludingInterner - newLocationSize;
    long saved = oldSize - newSizeIncludingInterner;
    double savedPercentage = oldSize == 0 ? 0 : saved * 100.0 / oldSize;
    System.out.printf(
        "PartitionLocation footprint [%s]: old=%d bytes, "
            + "newLocations=%d bytes, weakInternerOverhead=%d bytes, "
            + "newIncludingLiveInterner=%d bytes, savedIncludingLiveInterner=%d bytes (%.2f%%)%n",
        label,
        oldSize,
        newLocationSize,
        internerOverhead,
        newSizeIncludingInterner,
        saved,
        savedPercentage);
    if (newSizeIncludingInterner >= oldSize) {
      throw new AssertionError(
          "Optimized PartitionLocation retained size including its weak interner must be smaller for "
              + label);
    }
  }

  private long totalSize(Class<?> graphLayout, Object value) throws Exception {
    return totalSize(graphLayout, value, null);
  }

  private long totalSize(Class<?> graphLayout, Object value, Object additionalRoot)
      throws Exception {
    Method parseInstance = graphLayout.getMethod("parseInstance", Object[].class);
    Object[] roots =
        additionalRoot == null ? new Object[] {value} : new Object[] {value, additionalRoot};
    Object layout = parseInstance.invoke(null, new Object[] {roots});
    return (Long) graphLayout.getMethod("totalSize").invoke(layout);
  }

  private Object endpointInterner() throws Exception {
    Field field = WorkerEndpoint.class.getDeclaredField("INTERNED_ENDPOINTS");
    field.setAccessible(true);
    return field.get(null);
  }
}
