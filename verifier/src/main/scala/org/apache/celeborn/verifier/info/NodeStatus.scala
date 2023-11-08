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
package org.apache.celeborn.verifier.info

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.util.Utils
import org.apache.celeborn.verifier.conf.VerifierConf

class NodeStatus(
    var cores: Int,
    var masterAlive: Boolean,
    var workerAlive: Boolean,
    var workDirs: Map[String, WorkDirStatus]) extends Serializable with Logging {
  def this(values: (Int, Boolean, Boolean, Map[String, WorkDirStatus])) {
    this(values._1, values._2, values._3, values._4)
  }

  def this() {
    this(0, false, false, Map.empty[String, WorkDirStatus])
  }

  def checkAndUpdate(uid: String, other: NodeStatus): Unit = {
    if (this.masterAlive != other.masterAlive || this.workerAlive != other.workerAlive ||
      !this.workDirs.equals(other.workDirs))
      if (this.workDirs.equals(other.workDirs)) {
        logInfo(s"$uid resource stat mismatch," +
          s"current status : ${this.toString} ," +
          s"other status : ${other.toString}")
      }
  }

  override def toString: String = s"cores : $cores, " +
    s"masterAlive: $masterAlive, " +
    s"workerAlive: $workerAlive, " +
    s"dirs: $workDirs"

  def except(old: NodeStatus): NodeStatus = {
    new NodeStatus(this.cores - old.cores, old.masterAlive, old.workerAlive, old.workDirs)
  }

  override def equals(other: Any): Boolean = other match {
    case that: NodeStatus =>
      (that canEqual this) &&
        cores == that.cores &&
        masterAlive == that.masterAlive &&
        workerAlive == that.workerAlive &&
        workDirs == that.workDirs
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[NodeStatus]

  override def hashCode(): Int = {
    val state = Seq(cores, masterAlive, workerAlive, workDirs)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  def duplicate(): NodeStatus = {
    val nDisk = workDirs.map(it =>
      it._1 ->
        new WorkDirStatus(it._2.unwritable, it._2.hanging, it._2.deviceName))

    new NodeStatus(this.cores, this.masterAlive, this.workerAlive, nDisk)
  }
}

object NodeStatus {

  val DUMMY_RESOURCE = new NodeStatus()

  def apply(conf: VerifierConf): NodeStatus = {
    val checkWorkerRes = Utils.runCommand("ps -lef | grep Worker | grep -v grep")
    val checkMasterRes = Utils.runCommand("ps -lef | grep Master | grep -v grep")

    val workerExists =
      if (!checkWorkerRes.isEmpty) {
        true
      } else {
        if (VerifierConf.runnerIsTestMode(conf)) {
          true
        } else {
          false
        }
      }

    val masterExists =
      if (!checkMasterRes.isEmpty) {
        true
      } else {
        if (VerifierConf.runnerIsTestMode(conf)) {
          true
        } else {
          false
        }
      }

    val currentCpus = Runtime.getRuntime.availableProcessors();
    val workingDirs = conf.celebornConf.workerBaseDirs
    val workingDirToDeviceMap = workingDirDeviceMap(workingDirs.map(f => f._1).toList)

    // Todo check here

    val diskInfos = workingDirs.map(workDir =>
      workDir._1 -> new WorkDirStatus(false, false, workingDirToDeviceMap(workDir._1))).toMap

    new NodeStatus(currentCpus, masterExists, workerExists, diskInfos)
  }

  def workingDirDeviceMap(workingDirs: List[String]): Map[String, String] = {
    val cmdRes = Utils.runCommand("df -h ")
    val devicesAndMounts =
      cmdRes.split("[\n\r]").tail.map(_.trim.split("[ \t]+")).map(sp => (sp(0), sp(sp.size - 1)))

    workingDirs.map(workdir =>
      workdir -> devicesAndMounts.filter(dev => workdir.startsWith(dev._2)).head._1).toMap
  }

}
