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

import scala.annotation.tailrec

import org.apache.celeborn.common.util.{IntParam, Utils}
import org.apache.celeborn.verifier.conf.{VerifierArguments, VerifierConf}

class SchedulerArguments(override val args: Array[String], override val conf: VerifierConf)
  extends VerifierArguments(args, conf) {
  var host = Utils.localHostName()
  var port = 19097

  @tailrec
  final override protected def parse(args: List[String]): Unit = args match {
    case ("-h") :: value :: tail =>
      Utils.checkHost(value)
      host = value
      parse(tail)

    case ("-p") :: IntParam(value) :: tail =>
      port = value
      parse(tail)

    case ("--pf") :: value :: tail =>
      propertiesFile = value
      parse(tail)

    case Nil => // No-op

    case _ =>
      printUsageAndExit(1)
  }

  def printUsageAndExit(i: Int): Unit = {
    // scalastyle:off
    println(
      """
        | -h host
        | -p port
        | -pf property file path
        |""".stripMargin)
    // scalastyle:off
  }

}
