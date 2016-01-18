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
