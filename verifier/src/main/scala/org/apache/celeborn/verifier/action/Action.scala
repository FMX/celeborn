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
package org.apache.celeborn.verifier.action

import com.alibaba.fastjson.JSONObject

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.util.Utils
import org.apache.celeborn.verifier.conf.VerifierConf
import org.apache.celeborn.verifier.constant.{ActionTendencyConstant, DiskErrorOptConstant, ResourceTypeConstants}
import org.apache.celeborn.verifier.plan.exception.PlaninvalidException
import org.apache.celeborn.verifier.runner.RunnerContext
import org.apache.celeborn.verifier.scheduler.SchedulerContext

abstract class Action(val selector: Selector, val interval: Long)
  extends Serializable with Logging {
  val target: String = ResourceTypeConstants.TARGET_SCHEDULER
  val tendency = ActionTendencyConstant.NEUTUAL

  def generateOperations(context: SchedulerContext): List[Operation]

  def identity(): String

  def deduce(context: SchedulerContext, operations: List[Operation]): SchedulerContext = {
    val deduced = context.duplicate()
    operations.map(op => {
      val fakeRunnerContext =
        RunnerContext(context.conf, deduced.runners.get(op.target.id).resource)
      op.updateContextBlock(fakeRunnerContext)
    })
    deduced
  }

  override def toString: String = s"Action($target, $tendency, $selector, $interval, $identity)"
}

object Action {
  def parseJson(obj: JSONObject, conf: VerifierConf): Action = {
    val idOpt = Option(obj.getString("id"))
    if (idOpt.isEmpty) {
      throw new PlaninvalidException("action must define id")
    }

    val selectorObj = obj.getJSONObject("selector")
    val selector =
      if (selectorObj != null) {
        Selector.fromJson(selectorObj, conf)
      } else {
        Selector.dummy_selector
      }

    val interval =
      if (selectorObj.containsKey("interval")) {
        Utils.timeStringAsMs(selectorObj.getString("interval"))
      } else {
        Utils.timeStringAsMs(VerifierConf.getPlanActionDefaultInterval(conf))
      }

    idOpt.get match {
      case "stopmaster" => new StopMasterAction(selector, conf, interval)
      case "startmaster" => new StartMasterAction(selector, conf, interval)
      case "stopworker" => new StopWorkerAction(selector, conf, interval)
      case "startworker" => new StartWorkerAction(selector, conf, interval)
      case "corruptdisk" => new CorruptDiskAction(selector, interval)
      case "hangio" => new HangIoAction(selector, interval)
      case "resumedisk" => new ResumeDiskAction(selector, interval)
      case "resumeio" => new ResumeIoAction(selector, interval)
      case "occupycpu" =>
        val allowedDuration = Math.min(
          Utils.timeStringAsMs(Option(obj.getString("duration")).getOrElse("10s")),
          VerifierConf.getMaxOccupyCpuDurationMs(conf))

        new OccupyCpuAction(selector, obj.getIntValue("cores"), allowedDuration, interval)
      case unknown: String =>
        throw new PlaninvalidException(s"unknown action :${unknown} , plan is invalid")
    }
  }
}

case class ActionTarget(id: String, workDir: String)

class CorruptDiskAction(override val selector: Selector, override val interval: Long)
  extends DiskAction(selector, DiskErrorOptConstant.CORRUPT, true, false, interval) {
  override val tendency = ActionTendencyConstant.NEGATIVE

  override def identity(): String = "corruptdisk"
}

class CorruptMetaAction(override val selector: Selector, override val interval: Long)
  extends Action(selector, interval) {
  override val target: String = ResourceTypeConstants.TARGET_MASTER
  override val tendency: Int = ActionTendencyConstant.NEGATIVE

  override def generateOperations(context: SchedulerContext): List[Operation] = {
    val targets = selector.select(context, target)
    val ratisStorageDir = context.conf.celebornConf.haMasterRatisStorageDir
    val cmd = s"rm -rf ${ratisStorageDir}"
    targets.map(tar => new BashOperation(tar, cmd, interval = selector.interval))
  }

  override def identity(): String = "corruptmeta"
}

abstract class DiskAction(
    override val selector: Selector,
    diskCorruptOpt: String,
    status: Boolean,
    hangIo: Boolean,
    override val interval: Long)
  extends Action(selector, interval) {
  override val target: String = ResourceTypeConstants.TARGET_DISK

  override def generateOperations(context: SchedulerContext): List[Operation] = {
    if (selector.isInstanceOf[RandomSelector]) {
      selector.asInstanceOf[RandomSelector].updateSelectorStat(status, diskCorruptOpt)
    }
    val targets = selector.select(context, target)

    if (targets.isEmpty) {
      return List.empty[Operation]
    }
    targets.map(ta => {
      val corruptDiskCmd = s"sh chmod 400 ${ta.workDir}"
      val resumeDiskCmd = s"sh chmod 777 ${ta.workDir}"

      val corruptDiskBlock = (ctx: RunnerContext) => {
        ctx.resource.workDirs(ta.workDir).hanging = true
      }
      val resumeDiskBlock = (ctx: RunnerContext) => {
        ctx.resource.workDirs(ta.workDir).hanging = true
      }

      val systemBlock = context.conf.celebornConf.workerDiskMonitorSysBlockDir
      val deviceName = context.runners.get(ta.id).resource.workDirs(ta.workDir).deviceName
      val badInflightFile = VerifierConf.getBadInflightFile(context.conf)

      val hangIoDiskCmd = s"ln -sfn ${badInflightFile} ${systemBlock}/${deviceName}/inflight"
      val resumeIoHangCmd = s"ln -sfn ${systemBlock}/${deviceName}/inflight" +
        s" ${systemBlock}/${deviceName}/inflight "

      val hangIoBlock = (ctx: RunnerContext) => {
        ctx.resource.workDirs(ta.workDir).hanging = true
      }

      val resumeIoBlock = (ctx: RunnerContext) => {
        ctx.resource.workDirs(ta.workDir).hanging = false
      }

      val (cmd, block) = diskCorruptOpt match {
        case DiskErrorOptConstant.CORRUPT =>
          if (status) {
            (corruptDiskCmd, corruptDiskBlock)
          } else {
            (resumeDiskCmd, resumeDiskBlock)
          }
        case DiskErrorOptConstant.HANG =>
          if (hangIo) {
            (hangIoDiskCmd, hangIoBlock)
          } else {
            (resumeIoHangCmd, resumeIoBlock)
          }
      }

      new BashOperation(ta, cmd, block, selector.interval)
    })
  }

}

class HangIoAction(override val selector: Selector, override val interval: Long)
  extends DiskAction(selector, DiskErrorOptConstant.HANG, false, true, interval) {
  override val tendency = ActionTendencyConstant.NEGATIVE

  override def identity(): String = "hangio"
}

abstract class NodeAction(
    override val selector: Selector,
    conf: VerifierConf,
    toStart: Boolean,
    isMaster: Boolean,
    override val interval: Long)
  extends Action(selector, interval) {
  val startMasterScript: String = VerifierConf.getStartMasterScript(conf)
  val stopMasterScript: String = VerifierConf.getStopMasterScript(conf)
  val startWorkerScript: String = VerifierConf.getStartWorkerScript(conf)
  val stopWorkerScript: String = VerifierConf.getStopWorkerScript(conf)

  val startMasterCmd = s"sh ${startMasterScript}"
  val stopMasterCmd = s"sh ${stopMasterScript}"
  val startWorkerCmd = s"sh ${startWorkerScript}"
  val stopWorkerCmd = s"sh ${stopWorkerScript}"

  val runnerStartMaster = (ctx: RunnerContext) => {
    ctx.resource.masterAlive = true
  }

  val runnerStopMaster = (ctx: RunnerContext) => {
    ctx.resource.masterAlive = false
  }

  val runnerStartWorker = (ctx: RunnerContext) => {
    ctx.resource.workerAlive = true
  }

  val runnerStopWorker = (ctx: RunnerContext) => {
    ctx.resource.workerAlive = false
  }

  override def generateOperations(context: SchedulerContext): List[Operation] = {

    val (cmd, block) =
      if (isMaster) {
        if (toStart) {
          (startMasterCmd, runnerStartMaster)
        } else {
          (stopMasterCmd, runnerStopMaster)
        }
      } else {
        if (toStart) {
          (startWorkerCmd, runnerStartWorker)
        } else {
          (stopWorkerCmd, runnerStopWorker)
        }
      }

    if (selector.isInstanceOf[RandomSelector]) {
      selector.asInstanceOf[RandomSelector].updateSelectorStat(toStart, "")
    }
    val targets = selector.select(context, target)
    targets.map(it =>
      new BashOperation(it, cmd, block, selector.interval))
  }
}

class OccupyCpuAction(
    override val selector: Selector,
    cores: Int,
    duration: Long,
    override val interval: Long)
  extends Action(selector, interval) {
  override val tendency = ActionTendencyConstant.NEGATIVE
  override val target: String = ResourceTypeConstants.TARGET_RUNNER

  override def identity(): String = "occupycpu"

  override def generateOperations(context: SchedulerContext): List[Operation] = {
    val targets = selector.select(context, target)
    targets.map(node =>
      new OccupyCpuOperation(node, cores, duration, selector.interval))
  }

  override def deduce(context: SchedulerContext, operations: List[Operation]): SchedulerContext =
    context.duplicate()
}

class ResumeDiskAction(override val selector: Selector, override val interval: Long)
  extends DiskAction(selector, DiskErrorOptConstant.CORRUPT, false, false, interval) {
  override val tendency = ActionTendencyConstant.POSITIVE

  override def identity(): String = "resumedisk"
}

class ResumeIoAction(override val selector: Selector, override val interval: Long)
  extends DiskAction(selector, DiskErrorOptConstant.HANG, false, false, interval) {
  override val tendency = ActionTendencyConstant.POSITIVE

  override def identity(): String = "resumeio"
}

class StartMasterAction(
    override val selector: Selector,
    conf: VerifierConf,
    override val interval: Long)
  extends NodeAction(selector, conf, true, true, interval) {
  override val tendency: Int = ActionTendencyConstant.POSITIVE

  override val target: String = ResourceTypeConstants.TARGET_MASTER

  override def identity(): String = "startmaster"
}

class StartWorkerAction(
    override val selector: Selector,
    conf: VerifierConf,
    override val interval: Long)
  extends NodeAction(selector, conf, true, false, interval) {
  override val tendency: Int = ActionTendencyConstant.POSITIVE

  override val target: String = ResourceTypeConstants.TARGET_WORKER

  override def identity(): String = "startworker"
}

class StopMasterAction(
    override val selector: Selector,
    conf: VerifierConf,
    override val interval: Long)
  extends NodeAction(selector, conf, false, true, interval) {
  override val tendency: Int = ActionTendencyConstant.NEGATIVE

  override val target: String = ResourceTypeConstants.TARGET_MASTER

  override def identity(): String = "stopmaster"
}

class StopWorkerAction(
    override val selector: Selector,
    conf: VerifierConf,
    override val interval: Long)
  extends NodeAction(selector, conf, false, false, interval) {
  override val tendency: Int = ActionTendencyConstant.NEGATIVE

  override val target: String = ResourceTypeConstants.TARGET_WORKER

  override def identity(): String = "stopworker"
}
