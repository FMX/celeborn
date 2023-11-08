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
package org.apache.celeborn.verifier.scheduler

import java.util.concurrent.{ConcurrentHashMap, Future, ScheduledFuture, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import scala.collection.JavaConverters.{enumerationAsScalaIteratorConverter, mapAsScalaConcurrentMapConverter}
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.util.Random

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.rpc.{RpcCallContext, RpcEndpoint, RpcEnv}
import org.apache.celeborn.common.util.ThreadUtils
import org.apache.celeborn.verifier.action.Operation
import org.apache.celeborn.verifier.conf.VerifierConf
import org.apache.celeborn.verifier.constant.VerifierConstants
import org.apache.celeborn.verifier.info.{NodeStatus, RunnerInfo}
import org.apache.celeborn.verifier.plan.VerificationPlan
import org.apache.celeborn.verifier.plan.exception.PlanExecuteException
import org.apache.celeborn.verifier.protocol._

class Scheduler(override val rpcEnv: RpcEnv, val conf: VerifierConf)
  extends RpcEndpoint with Logging {

  // key : runnerId -> (resource,time,rpcEndpoint)
  private val runners = new ConcurrentHashMap[String, RunnerInfo]

  private val planExecutor = ThreadUtils.newDaemonSingleThreadExecutor("plan-executor")

  private val timeoutCheckerExecutor =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("scheduler-timeout-checker")
  private val stopPlanFlag = new AtomicBoolean(true)
  private val pausePlanFlag = new AtomicBoolean(false)
  private val runningPlan = new AtomicReference[VerificationPlan](null)
  private val timeoutThreshold = VerifierConf.getRunnerTimeOutMs(conf)
  private val resultExecutorPool = ThreadUtils.newDaemonFixedThreadPool(8, "result-execute-pool")
  private var planExecuteTask: Future[_] = _
  private var timeoutTask: ScheduledFuture[_] = _

  override def onStart(): Unit = {

    class PlanLoop extends Runnable {
      override def run(): Unit = {
        try {
          implicit val ec = ExecutionContext.fromExecutor(resultExecutorPool)
          val context = new SchedulerContext(conf, runners)
          while (true) {
            val plan = runningPlan.get()
            if (plan == null) {
              Thread.sleep(1000)
            } else {
              val trigger = plan.trigger
              val actions = plan.actions
              val clusterChecker = plan.clusterChecker
              val actionCount = actions.size

              var actIndex = 0
              var planLoopCount = 1

              val tendentiousActions = actions.groupBy(_.tendency)

              while (!stopPlanFlag.get()) {
                if (pausePlanFlag.get()) {
                  Thread.sleep(1000L)
                }
                val actionToExecute =
                  if (trigger.random) {
                    val actionCandidates = tendentiousActions(clusterChecker.tendency).filter(ac =>
                      clusterChecker.avaliableTargets.contains(ac.target))
                    val actionCandidatesSize = actionCandidates.size
                    actionCandidates(Random.nextInt(actionCandidatesSize))
                  } else {
                    logInfo(s"plan loop : ${planLoopCount}")
                    actions(actIndex)
                  }

                val operations = actionToExecute.generateOperations(context)
                logDebug(s"current action is :${actionToExecute} , " +
                  s"operations : ${operations.map(_.toString)}")
                val permissionToExecute =
                  if (operations.isEmpty) {
                    false
                  } else {
                    val deducedContext = actionToExecute.deduce(context, operations)
                    val permissionToRun = clusterChecker.validate(deducedContext)
                    logDebug(s"current context : ${context.toString}  ===> " +
                      s"deduced context  : ${deducedContext.toString} " +
                      s"current action execute : ${permissionToRun} ")
                    permissionToRun
                  }

                if (permissionToExecute) {
                  def executeOperationSync(operation: Operation): Unit = {
                    val operationResponseFuture =
                      runners.get(operation.target.id).rpcEndpointRef.ask[ExecuteOperationResponse](
                        ExecuteOperation(operation))
                    val response =
                      ThreadUtils.awaitReady(operationResponseFuture, Duration.Inf).value.get.get
                    if (response.statusCode != StatusCode.SUCCESS) {
                      throw new PlanExecuteException("Operation failed , plan execution abort.")
                    }

                    operation.updateSchedulerContext(context)

                    Thread.sleep(operation.interval)
                  }

                  operations.foreach(executeOperationSync(_))

                  Thread.sleep(actionToExecute.interval)
                }

                if (trigger.sequence) {
                  actIndex = actIndex + 1
                  if (actIndex >= actionCount - 1) {
                    planLoopCount = planLoopCount + 1
                    if (planLoopCount > trigger.repeat) {
                      logInfo("Plan execution completed. Clean plan.")
                      stopPlanFlag.set(true)
                      runningPlan.set(null)
                    } else {
                      actIndex = 0
                    }
                  }
                }

                Thread.sleep(trigger.interval.getInterval())

              }
            }
          }
        } catch {
          case t: Throwable =>
            try {
              logError("plan execute with exception , clean verification plan and log error ", t)
              planExecuteTask = planExecutor.submit(new PlanLoop)
              stopPlanFlag.set(true)
              runningPlan.set(null)
            } finally {
              throw t
            }
        }

      }
    }

    planExecuteTask = planExecutor.submit(new PlanLoop)

    timeoutTask = timeoutCheckerExecutor.schedule(
      new Runnable {
        override def run(): Unit = {
          self.send(CheckRunnerTimeout())
        }
      },
      30 * 1000,
      TimeUnit.MILLISECONDS)

  }

  override def onStop(): Unit = {
    if (planExecuteTask != null) {
      planExecuteTask.cancel(true)
    }
    planExecutor.shutdownNow()

    if (timeoutTask != null) {
      timeoutTask.cancel(true)
    }
    timeoutCheckerExecutor.shutdownNow()
  }

  override def receive: PartialFunction[Any, Unit] = {
    case CheckRunnerTimeout() =>
      logTrace("received timeout check")
      handleRunnerTimeout()
    case RunnerHeartBeat(uid, resource, diff) =>
      logTrace(s"received runner ${uid} heartbeat")
      handleRunnerHeartBeat(uid, resource, diff)
  }

  def handleRunnerTimeout(): Unit = {
    val outdatedItems = runners.asScala.filter(k =>
      (System.currentTimeMillis() - k._2.lastContactTime > timeoutThreshold)).toList
    if (!outdatedItems.isEmpty) {
      logError(s"Fatal error occurred,but got : ${outdatedItems}")
      stopPlanFlag.set(true)
    }
  }

  def handleRunnerHeartBeat(uid: String, resource: NodeStatus, diff: NodeStatus): Unit = {
    if (runners.containsKey(uid)) {
      if (!VerifierConf.runnerIsTestMode(conf)) {
        runners.get(uid).resource.checkAndUpdate(uid, resource)
      }
      runners.get(uid).lastContactTime = System.currentTimeMillis()
    } else {
      logTrace(s"current all runners : ${runners.keys().asScala.toList}")
      logWarning(s"received unknown worker ${uid}")
    }

  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RegisterRunner(uid, resource) =>
      logTrace("received register runner")
      handleRegisterRunner(uid, resource, context)
    case SubmitPlan(plan) =>
      logTrace("received submit plan")
      handleSubmitPlan(plan, context)
    case PausePlan() =>
      logTrace("received pause plan")
      handlePausePlan(context)
    case ResumePlan() =>
      logTrace("received resume plan")
      handleResumePlan(context)
    case StopPlan() =>
      logTrace("received stop plan")
      handleStopPlan(context)
    case QueryStatus() =>
      logTrace("received query status")
      handleQueryStatus(context)
  }

  def handleQueryStatus(context: RpcCallContext): Unit = {
    val lineSeparate = System.lineSeparator()

    val report = new StringBuilder
    report.append("***  verifier status report ***")
    report.append(lineSeparate)
    if (runningPlan.get() != null) {
      report.append(s"current active plan : ${runningPlan.get()}")
    } else {
      report.append(s"idle stat ")
    }

    report.append(lineSeparate)
    report.append(s"here are ${this.runners.size()} runners online:")
    report.append(lineSeparate)

    this.runners.asScala.foreach(it => {
      report.append(s"runner:${it._1} detail : ${it._2.toString} ")
      report.append(lineSeparate)
    })

    context.reply(QueryStatusResponse(StatusCode.SUCCESS, null, report.toString()))
  }

  def handleStopPlan(context: RpcCallContext): Unit = {
    this.stopPlanFlag.set(true)
    this.runningPlan.set(null)
    Thread.sleep(1000)

    context.reply(new StopPlanResponse(StatusCode.SUCCESS, null))
  }

  def handleRegisterRunner(uid: String, resource: NodeStatus, context: RpcCallContext): Unit = {
    runners.synchronized {
      val runnerEp = rpcEnv.setupEndpointRef(context.senderAddress, VerifierConstants.RUNNER_EP)
      runners.put(uid, new RunnerInfo(resource, System.currentTimeMillis(), runnerEp))
      logTrace(s"register runner : ${uid}")
      context.reply(RegisterRunnerResponse(StatusCode.SUCCESS, null))
    }
  }

  def handleResumePlan(context: RpcCallContext): Unit = {
    this.pausePlanFlag.set(false)
    context.reply(ResumePlanResponse(StatusCode.SUCCESS, null))
  }

  def handlePausePlan(context: RpcCallContext): Unit = {
    this.pausePlanFlag.set(true)
    context.reply(PausePlanResponse(StatusCode.SUCCESS, null))
  }

  def handleSubmitPlan(plan: VerificationPlan, context: RpcCallContext): Unit = {
    if (this.runningPlan.get() != null || !this.stopPlanFlag.get()) {
      logWarning("there is a plan running already ")
      context.reply(SubmitPlanResponse(
        StatusCode.REQUEST_FAILED,
        new IllegalStateException("there is a active plan already , stop it first")))
    } else {
      this.stopPlanFlag.set(false)
      this.runningPlan.set(plan)
      context.reply(SubmitPlanResponse(StatusCode.SUCCESS, null))
      logInfo(s"submit plan : ${plan.toString}")
    }
  }
}

object Scheduler {
  def main(args: Array[String]): Unit = {
    val conf = new VerifierConf()

    val parsedArgs = new SchedulerArguments(args, conf)
    val rpcEnv = RpcEnv.create(
      VerifierConstants.SCHEDULER_SYS,
      parsedArgs.host,
      parsedArgs.port,
      conf.celebornConf)

    val scheduler = new Scheduler(rpcEnv, conf)
    rpcEnv.setupEndpoint(VerifierConstants.SCHEDULER_EP, scheduler)

    rpcEnv.awaitTermination()
  }
}
