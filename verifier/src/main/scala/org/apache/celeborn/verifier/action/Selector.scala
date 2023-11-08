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

import scala.collection.JavaConverters.{collectionAsScalaIterableConverter, enumerationAsScalaIteratorConverter, mapAsScalaConcurrentMapConverter}
import scala.util.Random

import com.alibaba.fastjson.JSONObject

import org.apache.celeborn.common.util.Utils
import org.apache.celeborn.verifier.conf.VerifierConf
import org.apache.celeborn.verifier.constant.DiskErrorOptConstant
import org.apache.celeborn.verifier.constant.ResourceTypeConstants.{TARGET_DISK, TARGET_MASTER, TARGET_RUNNER, TARGET_WORKER}
import org.apache.celeborn.verifier.plan.exception.PlaninvalidException
import org.apache.celeborn.verifier.scheduler.SchedulerContext

abstract class Selector(val interval: Long) extends Serializable {
  val skipInterval = getInterval() == 0

  def select(schedulerContext: SchedulerContext, target: String): List[ActionTarget]

  def getInterval(): Long = this.interval

  def getItemListByIndices[T](elems: List[T], index: List[Int]): List[T] = {
    index.map(index => index -> elems(index)).map(_._2)
  }

  override def toString: String = s"Selector($skipInterval, $interval)"

  protected def getAllNodes(schedulerContext: SchedulerContext): List[String] = {
    schedulerContext.runners.asScala.map(_._1).toList
  }

  protected def getMasterNodeListByState(
      context: SchedulerContext,
      statusToBe: Boolean): List[String] = {
    context.runners.asScala.filter(_._2.resource.masterAlive != statusToBe).map(_._1).toList
  }

  protected def getWorkerNodeListByState(
      context: SchedulerContext,
      statusToBe: Boolean): List[String] = {
    context.runners.asScala.filter(_._2.resource.workerAlive != statusToBe).map(_._1).toList
  }

  protected def getDisksByState(
      context: SchedulerContext,
      diskErrorOpt: String,
      statusToBe: Boolean): List[(String, String)] = {
    val diskWithStatus = context.runners.asScala.flatMap(node =>
      (node._2.resource.workDirs.toList.map(tup => (node._1, tup._1, tup._2)))).toList

    val ioHangDisks = diskWithStatus.filter(_._3.hanging)
    val unwritableDisks = diskWithStatus.filter(_._3.unwritable)
    val writableDisks = diskWithStatus.filter(_._3.unwritable == false)
    val ioNormalDisks = diskWithStatus.filter(_._3.hanging == false)

    val res = diskErrorOpt match {
      case DiskErrorOptConstant.CORRUPT =>
        if (statusToBe) {
          writableDisks
        } else {
          unwritableDisks
        }
      case DiskErrorOptConstant.HANG =>
        if (statusToBe) {
          ioNormalDisks
        } else {
          ioHangDisks
        }
    }

    res.map(item => (item._1, item._2))

  }
}

object Selector {

  val dummy_selector = new DummySelector(0)
  val dummy_node = "dummy_node"
  val dummy_disk = "dummy_disk"

  def fromJson(obj: JSONObject, conf: VerifierConf): Selector = {
    val sType = Option(obj.getString("type")).getOrElse("assign")

    val intervalInMs = Utils.timeStringAsMs(Option(obj.getString("interval")).getOrElse(
      VerifierConf.getPlanActionSelectorDefaultInterval(conf)))
    sType match {
      case "assign" =>
        val indices = Option(obj.getJSONArray("indices")).get
          .asScala.map(_.asInstanceOf[Int]).toList
        val devices =
          if (obj.containsKey("device")) {
            Option(obj.getJSONArray("device")).get
              .asScala.map(_.asInstanceOf[Int]).toList
          } else {
            List.empty[Int]
          }
        new AssignSelector(intervalInMs, indices, devices)
      case "random" =>
        new RandomSelector(intervalInMs)
      case unknown: String =>
        throw new PlaninvalidException(s"selector only support assign now , but got ${unknown}")
    }
  }
}

class AssignSelector(override val interval: Long, val indices: List[Int], val devices: List[Int])
  extends Selector(interval) {
  def select(schedulerContext: SchedulerContext, target: String): List[ActionTarget] = {

    val masters = getAllNodes(schedulerContext)

    val workers = getAllNodes(schedulerContext)

    val targets = target match {
      case TARGET_WORKER =>
        getItemListByIndices(workers, indices).map(node =>
          ActionTarget(node, Selector.dummy_disk))

      case TARGET_MASTER =>
        getItemListByIndices(masters, indices).map(node =>
          ActionTarget(node, Selector.dummy_disk))

      case TARGET_DISK =>
        getItemListByIndices(workers, indices).flatMap(node =>
          getItemListByIndices(
            schedulerContext.runners.get(node).resource.workDirs.keys.toList,
            devices).map(workDir => ActionTarget(node, workDir)))

      case TARGET_RUNNER =>
        getItemListByIndices(schedulerContext.runners.keys().asScala.toList, indices).map(node =>
          ActionTarget(node, Selector.dummy_disk))
    }
    targets
  }

  override def getInterval(): Long = this.interval
}

class DummySelector(override val interval: Long) extends Selector(interval) {
  override def select(schedulerContext: SchedulerContext, target: String): List[ActionTarget] = {
    List.empty[ActionTarget]
  }

  override def getInterval(): Long = interval
}

class RandomSelector(override val interval: Long) extends Selector(interval) {
  private var statusToBe: Boolean = _
  private var diskErrorOperation: String = _

  def updateSelectorStat(status: Boolean, diskErrorOperation: String): Unit = {
    this.statusToBe = status
    this.diskErrorOperation = diskErrorOperation
  }

  override def select(schedulerContext: SchedulerContext, target: String): List[ActionTarget] = {
    val targets = target match {
      case TARGET_MASTER =>
        val requiredMasters = getMasterNodeListByState(schedulerContext, statusToBe)
        ActionTarget(
          randomSelectOneItem(requiredMasters, TARGET_MASTER, statusToBe.toString),
          Selector.dummy_disk)

      case TARGET_WORKER =>
        val requiredWorkers = getWorkerNodeListByState(schedulerContext, statusToBe)
        ActionTarget(
          randomSelectOneItem(requiredWorkers, TARGET_WORKER, statusToBe.toString),
          Selector.dummy_disk)

      case TARGET_DISK =>
        val requiredDisks = getDisksByState(schedulerContext, diskErrorOperation, statusToBe)
        if (requiredDisks.isEmpty) {
          null
        } else {
          val selectedDisk =
            randomSelectOneItem(requiredDisks, TARGET_DISK, diskErrorOperation, statusToBe.toString)
          ActionTarget(selectedDisk._1, selectedDisk._2)
        }

      case TARGET_RUNNER =>
        val runners = schedulerContext.runners.keys().asScala.toList
        val selectedRunner =
          randomSelectOneItem(runners, TARGET_RUNNER, diskErrorOperation, statusToBe.toString)
        ActionTarget(selectedRunner, Selector.dummy_disk)
    }
    if (targets == null) {
      List.empty[ActionTarget]
    } else {
      List(targets)
    }
  }

  def randomSelectOneItem[T](list: List[T], target: String*): T = {
    if (list.size == 0) {
      null
    }
    list(Random.nextInt(list.size))
  }
}
