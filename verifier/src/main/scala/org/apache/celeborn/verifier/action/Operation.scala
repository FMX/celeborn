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

import java.util.concurrent.atomic.AtomicBoolean

import scala.util.Random

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.util.Utils
import org.apache.celeborn.verifier.runner.RunnerContext
import org.apache.celeborn.verifier.scheduler.SchedulerContext

abstract class Operation(
    val target: ActionTarget,
    val updateContextBlock: (RunnerContext => Unit) = ctx => {},
    val interval: Long)
  extends Serializable with Logging {

  def executeOnRunner(context: RunnerContext): OperationResult

  def updateSchedulerContext(context: SchedulerContext): Unit = {
    val runContext = RunnerContext(context.conf, context.runners.get(target.id).resource)
    updateContextBlock(runContext)
  }

  def runCmdGetResult(cmd: String, block: => Unit): OperationResult = {
    try {
      val cmdRes = Utils.runCommand(cmd)
      logDebug(s"cmd : ${cmd} executed , result : ${cmdRes}")
      block
      new OperationResult(cmdRes, null)
    } catch {
      case e: Exception =>
        new OperationResult(null, e)
    }
  }

  override def toString: String = s"Operation($target, $interval)"
}

class BashOperation(
    override val target: ActionTarget,
    val cmd: String,
    override val updateContextBlock: (RunnerContext => Unit) = (_) => {},
    override val interval: Long) extends Operation(target, interval = interval) {

  override def executeOnRunner(context: RunnerContext): OperationResult = {
    runCmdGetResult(cmd, updateContextBlock)
  }
}

class OccupyCpuOperation(
    override val target: ActionTarget,
    val cores: Int,
    val duration: Long,
    override val interval: Long)
  extends Operation(target, interval = interval) {

  val MAX_SLEEP_TIME = 5 * 60 * 1000

  var c: Long = 0

  override def executeOnRunner(context: RunnerContext): OperationResult = {
    try {
      val threadFlag = new AtomicBoolean(true)
      val threadNum = cores

      val threads = (1 to threadNum).map(i => new Thread(new CpuConsumer(threadFlag))).toList
      threads.foreach(_.start())
      val safetyDuration =
        if (duration > MAX_SLEEP_TIME) {
          MAX_SLEEP_TIME
        } else {
          this.duration
        }
      Thread.sleep(safetyDuration)
      threadFlag.set(false)
      new OperationResult("success", null)
    } catch {
      case e: Exception =>
        new OperationResult(null, e)
    }
  }

  class CpuConsumer(val flag: AtomicBoolean) extends Runnable {
    override def run(): Unit = {
      val random = Random
      while (flag.get()) {
        val a = random.nextInt()
        val b = random.nextInt()
        if (b != 0) {
          c = a + b * a / b
        } else {
          c = a + b * a
        }
      }
    }
  }

}

class OperationResult(val result: String, val exception: Exception) {}
