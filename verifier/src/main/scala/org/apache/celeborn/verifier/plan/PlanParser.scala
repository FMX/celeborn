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

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

import com.alibaba.fastjson.{JSON, JSONObject}

import org.apache.celeborn.verifier.action.Action
import org.apache.celeborn.verifier.conf.VerifierConf
import org.apache.celeborn.verifier.constant.PlanKeyConstants._
import org.apache.celeborn.verifier.plan.exception.PlaninvalidException

object PlanParser {

  def parse(planStr: String, conf: VerifierConf): VerificationPlan = {
    val planJsonObj = JSON.parseObject(planStr)

    val actionArrayObj = planJsonObj.getJSONArray(KEY_ACTIONS)
    val actions = actionArrayObj.asScala.map(obj => {
      val actionJsonObj = obj.asInstanceOf[JSONObject]
      Action.parseJson(actionJsonObj, conf)
    }).toList

    val triggerPolicyObj = planJsonObj.getJSONObject(KEY_TRIGGER)

    val trigger = Trigger.fromJson(triggerPolicyObj)

    if (trigger.random &&
      actions.map(_.identity()).find(_.equalsIgnoreCase("corruptmeta")).isDefined) {
      throw new PlaninvalidException("random trigger can not support corruptmeta action ")
    }

    val checkerStr = Option(planJsonObj.getString(KEY_CHECKER))
    val clusterChecker = Checker.getChecker(checkerStr)

    new VerificationPlan(actions, trigger, clusterChecker)

  }

}
