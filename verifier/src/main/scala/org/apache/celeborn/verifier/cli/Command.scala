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
package org.apache.celeborn.verifier.cli

import scala.io.Source

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.rpc.RpcEndpointRef
import org.apache.celeborn.verifier.cli.Command.verifyResult
import org.apache.celeborn.verifier.conf.VerifierConf
import org.apache.celeborn.verifier.plan.PlanParser
import org.apache.celeborn.verifier.protocol._

trait Command extends Logging {
  def init(args: Array[String])

  def execute(commandContext: CommandContext)
}

object Command extends Logging {
  def verifyResult(response: ResponseMessage, commandName: String): Unit = {
    if (StatusCode.SUCCESS == response.statusCode) {
      logInfo(s"command ${commandName} success ")
    } else {
      logError(s"command ${commandName} failed ", response.exception)
    }
  }
}

case class CommandContext(conf: VerifierConf)

case class PausePlanCommand(rpcEndpointRef: RpcEndpointRef) extends Command {

  override def init(args: Array[String]): Unit = {}

  override def execute(commandContext: CommandContext): Unit = {
    val pausePlanRes = rpcEndpointRef.askSync[PausePlanResponse](PausePlan())
    verifyResult(pausePlanRes, "PausePlan")
  }
}

case class ResumePlanCommand(rpcEndpointRef: RpcEndpointRef) extends Command {

  override def init(args: Array[String]): Unit = {}

  override def execute(commandContext: CommandContext): Unit = {
    val resumePlanRes = rpcEndpointRef.askSync[ResumePlanResponse](ResumePlan())
    verifyResult(resumePlanRes, "ResumePlan")
  }
}

case class SubmitCommand(rpcEndpointRef: RpcEndpointRef) extends Command {

  var planFileLocation: String = _

  override def init(args: Array[String]): Unit = {
    assert(args.size >= 0)
    planFileLocation = args(0)
  }

  override def execute(commandContext: CommandContext): Unit = {
    var source: Source = null
    try {
      source = Source.fromFile(planFileLocation)
      val plan = PlanParser.parse(source.mkString, commandContext.conf)
      logDebug(s"current plan : ${plan.toString}")
      val resp = rpcEndpointRef.askSync[SubmitPlanResponse](SubmitPlan(plan))
      verifyResult(resp, "submitPlan")
    } catch {
      case e1: Exception =>
        logError("Parse plan file failed ", e1)
    } finally {
      if (source != null) {
        source.close()
      }
    }
  }
}

case class ReportCommand(rpcEndpointRef: RpcEndpointRef) extends Command {
  override def init(args: Array[String]): Unit = {}

  override def execute(commandContext: CommandContext): Unit = {
    val queryStatusResponse = rpcEndpointRef.askSync[QueryStatusResponse](QueryStatus())
    verifyResult(queryStatusResponse, "QueryReport")
    if (queryStatusResponse.report != null) {
      logInfo(queryStatusResponse.report)
    }
  }
}

case class StopCommand(rpcEndpointRef: RpcEndpointRef) extends Command {
  override def init(args: Array[String]): Unit = {}

  override def execute(commandContext: CommandContext): Unit = {
    val queryStatusResponse = rpcEndpointRef.askSync[QueryStatusResponse](QueryStatus())
    verifyResult(queryStatusResponse, "StopPlan")
    if (queryStatusResponse.report != null) {
      logInfo(queryStatusResponse.report)
    }
  }
}
