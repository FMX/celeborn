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

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.rpc.{RpcAddress, RpcEnv}
import org.apache.celeborn.common.util.Utils
import org.apache.celeborn.verifier.conf.VerifierConf
import org.apache.celeborn.verifier.constant.VerifierConstants

object Cli extends Logging {
  def main(args: Array[String]): Unit = {
    if (args.size < 3) {
      logInfo(
        """
          |usage : schedulerHost schedulerPort cmd args*
          |available commands :
          |pause
          |resume
          |submit
          |report
          |""".stripMargin)
    }

    val (host, port, cmd) = (args(0), args(1).toInt, args(2))

    val conf = new VerifierConf()

    val rpcEnv = RpcEnv.create(
      VerifierConstants.CLI_SYS,
      Utils.localHostName(),
      0,
      conf.celebornConf)

    val actualHost =
      if (host == "localhost") {
        Utils.localHostName()
      } else {
        host
      }

    val schedulerRef =
      rpcEnv.setupEndpointRef(RpcAddress(actualHost, port), VerifierConstants.SCHEDULER_EP)

    val commandToRun = cmd match {
      case "pause" => PausePlanCommand(schedulerRef)
      case "resume" => ResumePlanCommand(schedulerRef)
      case "submit" => SubmitCommand(schedulerRef)
      case "report" => ReportCommand(schedulerRef)
      case "stop" => StopCommand(schedulerRef)
    }

    val commandContext = CommandContext(conf)

    commandToRun.init(args.slice(3, args.size))

    commandToRun.execute(commandContext)

  }
}
