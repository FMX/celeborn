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
package org.apache.celeborn.verifier.conf

import java.io.File
import java.util.HashMap

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.rpc.RpcAddress
import org.apache.celeborn.common.util.Utils

class VerifierConf() {
  val celebornConf = new CelebornConf()
  val settings = new HashMap[String, String]()

  def get(key: String, defaultValue: String): String = {
    Option(settings.get(key)).getOrElse(defaultValue)
  }

  def set(key: String, value: String): Unit = {
    settings.put(key, value)
  }

  def getCelebornConf() = celebornConf
}

object VerifierConf {

  val defaultScriptsLocation: String = {
    sys.env.get("ESS_HOME").map { t => s"$t${File.separator}sbin" }
      .map { t => new File(s"$t") }
      .filter(_.isFile)
      .map(_.getAbsolutePath)
      .orNull
  }

  def runnerIsTestMode(conf: VerifierConf): Boolean = {
    conf.get("verf.runner.test.mode", "false").toBoolean
  }

  def getMasterRpcAddress(conf: VerifierConf): RpcAddress = {
    val parts = conf.get(
      "verf.scheduler.address",
      Utils.localHostName(conf.celebornConf) + ":" + 19097).split(":")
    RpcAddress(parts(0), parts(1).toInt)
  }

  def getActionPackageNames(conf: VerifierConf): List[String] = {
    val packages = conf.get("verf.action.jars", "")
    packages.split(",").toList
  }

  def getStartMasterScript(conf: VerifierConf): String = {
    conf.get(
      "verf.scripts.master.start.script",
      s"${defaultScriptsLocation}${File.separator}start-master.sh")
  }

  def getStopMasterScript(conf: VerifierConf): String = {
    conf.get(
      "verf.scripts.master.stop.script",
      s"${defaultScriptsLocation}${File.separator}start-master.sh")
  }

  def getStartWorkerScript(conf: VerifierConf): String = {
    conf.get(
      "verf.scripts.worker.start.script",
      s"${defaultScriptsLocation}${File.separator}start-master.sh")
  }

  def getStopWorkerScript(conf: VerifierConf): String = {
    conf.get(
      "verf.scripts.worker.stop.script",
      s"${defaultScriptsLocation}${File.separator}start-master.sh")
  }

  def getBadInflightFile(conf: VerifierConf): String = {
    conf.get("verf.block.bad.inflight.location", s"/root/badblock/inflight")
  }

  def getRunnerTimeOutMs(conf: VerifierConf): Long = {
    Utils.timeStringAsMs(conf.get("verf.runner.timeout", "120s"))
  }

  def getRunnerRegisterRetryDelay(conf: VerifierConf): Long = {
    Utils.timeStringAsMs(conf.get("verf.runner.register.retry.delay", "5s"))
  }

  def getRunnerHeartBeatDelayMs(conf: VerifierConf): Long = {
    Utils.timeStringAsMs(conf.get("verf.runner.heartbeat.delay", "30s"))
  }

  def getRunnerHeartBeatIntervalMs(conf: VerifierConf): Long = {
    Utils.timeStringAsMs(conf.get("verf.runner.heartbeat.interval", "30s"))
  }

  def getPlanActionSelectorDefaultInterval(conf: VerifierConf): String = {
    conf.get("verf.plan.action.selector.default.interval", "5s")
  }

  def getPlanActionDefaultInterval(conf: VerifierConf): String = {
    conf.get("verf.plan.action.default.interval", "5s")
  }

  def getMaxOccupyCpuDurationMs(conf: VerifierConf): Long = {
    Utils.timeStringAsMs(conf.get("verf.plan.action.occupycpu.maxduration", "120s"))
  }

}
