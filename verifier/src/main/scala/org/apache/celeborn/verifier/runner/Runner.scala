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
package org.apache.celeborn.verifier.runner

import java.util.UUID
import java.util.concurrent.{ScheduledFuture, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.rpc.{RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.celeborn.common.util.{ThreadUtils, Utils}
import org.apache.celeborn.verifier.action.Operation
import org.apache.celeborn.verifier.conf.VerifierConf
import org.apache.celeborn.verifier.constant.VerifierConstants
import org.apache.celeborn.verifier.info.NodeStatus
import org.apache.celeborn.verifier.protocol._

class Runner(override val rpcEnv: RpcEnv, val conf: VerifierConf)
  extends RpcEndpoint with Logging {

  val heartBeatThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("Runner-Heartbeat-Thread")
  val heartBeatDelayTime = VerifierConf.getRunnerHeartBeatDelayMs(conf)
  val heartBeatIntervalTime = VerifierConf.getRunnerHeartBeatIntervalMs(conf)
  val runnerId: String = UUID.randomUUID().toString
  logInfo(s"current node id : ${runnerId}")
  val registerRetryTime = VerifierConf.getRunnerRegisterRetryDelay(conf)
  val currentResource = new AtomicReference[NodeStatus](NodeStatus(conf))
  var masterRef: RpcEndpointRef = _
  var heartBeatTask: ScheduledFuture[_] = _

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case ExecuteOperation(operation) =>
      logDebug(s"received action ${operation}")
      handleDoAction(operation, context)
  }

  def handleDoAction(operation: Operation, context: RpcCallContext): Unit = {

    logDebug(s"execute ${operation} at ${System.currentTimeMillis()}")
    operation.executeOnRunner(RunnerContext(conf, currentResource.get()))

    context.reply(ExecuteOperationResponse(
      StatusCode.SUCCESS,
      null,
      this.currentResource.get(),
      this.runnerId))

  }

  override def onStart(): Unit = {
    masterRef = rpcEnv.setupEndpointRef(
      VerifierConf.getMasterRpcAddress(conf),
      VerifierConstants.SCHEDULER_EP)

    val registerRes = masterRef.askSync[RegisterRunnerResponse](RegisterRunner(
      runnerId,
      this.currentResource.get()))

    if (registerRes.statusCode != StatusCode.SUCCESS) {
      Thread.sleep(registerRetryTime)
      val retryRegister = masterRef.askSync[RegisterRunnerResponse](RegisterRunner(
        runnerId,
        this.currentResource.get()))

      if (retryRegister.statusCode != StatusCode.SUCCESS) {
        sys.error("register runner to scheduler failed , runner abort.")
        sys.exit(1)
      }

    }

    logInfo("register with scheduler success")

    heartBeatTask = heartBeatThread.scheduleAtFixedRate(
      new Runnable {
        override def run(): Unit = {
          Utils.tryLogNonFatalError {
            val newResource = NodeStatus(conf)
            val oldResource = currentResource.getAndSet(NodeStatus(conf))

            masterRef.send(RunnerHeartBeat(
              runnerId,
              NodeStatus(conf),
              newResource.except(oldResource)))
            logDebug(s"check resource and send heartbeat ")
          }
        }
      },
      heartBeatDelayTime,
      heartBeatIntervalTime,
      TimeUnit.MILLISECONDS)

  }

  override def onStop(): Unit = {
    if (heartBeatTask != null) {
      heartBeatTask.cancel(true)
    }
    heartBeatThread.shutdownNow()
  }

}

object Runner {
  def main(args: Array[String]): Unit = {
    val conf = new VerifierConf()

    val parsedArgs = new RunnerArguments(args, conf)
    val rpcEnv = RpcEnv.create(
      VerifierConstants.RUNNER_SYS,
      parsedArgs.host,
      0,
      conf.celebornConf)

    val runner = new Runner(rpcEnv, conf)
    rpcEnv.setupEndpoint(VerifierConstants.RUNNER_EP, runner)

    rpcEnv.awaitTermination()
  }
}
