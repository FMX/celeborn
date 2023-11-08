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
package org.apache.celeborn.verifier.plan

import java.util
import java.util.HashSet

import scala.collection.JavaConverters.{mapAsScalaConcurrentMapConverter, seqAsJavaListConverter}

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.verifier.constant.ActionTendencyConstant
import org.apache.celeborn.verifier.constant.ResourceTypeConstants._
import org.apache.celeborn.verifier.scheduler.SchedulerContext

trait Checker extends Serializable with Logging {
  def validate(deducedContext: SchedulerContext): Boolean

  def tendency: Int

  def avaliableTargets: util.HashSet[String]

}

object Checker {
  val DUMMY_CHECKER = new DummyChecker()

  def getChecker(str: Option[String]): Checker = {
    if (str.isEmpty) {
      return DUMMY_CHECKER
    }
    str.get match {
      case "resource" => new ResourceChecker
      case _ => DUMMY_CHECKER
    }
  }
}

class DummyChecker extends Checker {
  override def validate(deducedContext: SchedulerContext): Boolean = true

  override def tendency: Int = 0

  override def avaliableTargets: util.HashSet[String] = new util.HashSet[String]()
}

class ResourceChecker extends Checker {
  override val avaliableTargets: HashSet[String] = new HashSet[String](
    List(
      TARGET_RUNNER,
      TARGET_SCHEDULER,
      TARGET_WORKER,
      TARGET_MASTER,
      TARGET_DISK,
      TARGET_CPU).asJava)
  var tendency: Int = ActionTendencyConstant.NEGATIVE

  override def validate(deducedContext: SchedulerContext): Boolean = {
    validateSingleContext(deducedContext)
  }

  def validateSingleContext(context: SchedulerContext): Boolean = {

    val aliveMaster = context.runners.asScala.filter(_._2.resource.masterAlive).map(_._1)
    val aliveWorker = context.runners.asScala.filter(_._2.resource.workerAlive).map(_._1)

    val masterExhausted = aliveMaster.size <= context.runners.size() / 2
    val workerExhausted = aliveWorker.size <= 2
    val workDirExhausted = aliveWorker.map(
      context.runners.get(_).resource.workDirs.map(_._2.available()).reduce(_ || _)).toList.filter(
      p => p).size <= 2

    if (tendency == ActionTendencyConstant.NEGATIVE) {
      if (masterExhausted) {
        avaliableTargets.remove(TARGET_MASTER)
      }
      if (workerExhausted) {
        avaliableTargets.remove(TARGET_WORKER)
      }
      if (workDirExhausted) {
        avaliableTargets.remove(TARGET_DISK)
      }
    }

    val resourceExhausted = masterExhausted && workerExhausted && workDirExhausted
    if (resourceExhausted && tendency != ActionTendencyConstant.POSITIVE) {
      logDebug(s"checker tendency changed from NEGATIVE to POSITIVE")
      tendency = ActionTendencyConstant.POSITIVE
      avaliableTargets.add(TARGET_MASTER)
      avaliableTargets.add(TARGET_WORKER)
      avaliableTargets.add(TARGET_DISK)
    }

    val masterFull = aliveMaster.size == context.runners.size()
    val workerFull = aliveWorker.size == context.runners.size()
    val workDirFull = aliveWorker.map(
      context.runners.get(_).resource.workDirs.map(_._2.available()).reduce(_ && _)).reduce(_ && _)

    if (tendency == ActionTendencyConstant.POSITIVE) {
      if (masterFull) {
        avaliableTargets.remove(TARGET_MASTER)
      }
      if (workerFull) {
        avaliableTargets.remove(TARGET_WORKER)
      }
      if (workDirFull) {
        avaliableTargets.remove(TARGET_DISK)
      } else {
        avaliableTargets.add(TARGET_DISK)
      }
    }

    val reachedMaxmum = masterFull && workerFull && workDirFull
    if (reachedMaxmum && tendency != ActionTendencyConstant.NEGATIVE) {
      logDebug(s"checker tendency changed from POSITIVE to NEGATIVE")
      tendency = ActionTendencyConstant.NEGATIVE
      avaliableTargets.add(TARGET_MASTER)
      avaliableTargets.add(TARGET_WORKER)
      avaliableTargets.add(TARGET_DISK)
    }

    !resourceExhausted
  }
}
