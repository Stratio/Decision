/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.decision.api.messaging

import java.util

import com.stratio.decision.api.messaging.MessageBuilder._
import com.stratio.decision.commons.constants.ENGINE_ACTIONS_PARAMETERS._

/**
  * Created by josepablofernandez on 21/12/15.
  */
class DroolsMessageBuilder(streamName: String, operation: String){

  def build(groupName: String, outputStream: String = null, kafkaTopic: String = null) = {

    val additionalParameters : java.util.Map[String, Object] = new util.HashMap[String, Object]()

    additionalParameters.put(DROOLS.GROUP, groupName)
    additionalParameters.put(DROOLS.CEP_OUTPUT_STREAM, outputStream)
    additionalParameters.put(DROOLS.KAFKA_OUTPUT_TOPIC, kafkaTopic)

    builder.withStreamName(streamName)
      .withOperation(operation)
      .withAdditionalParameters(additionalParameters)
      .build()
  }

}
