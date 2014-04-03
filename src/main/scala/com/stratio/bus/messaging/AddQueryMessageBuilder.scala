package com.stratio.bus.messaging

import com.stratio.streaming.commons.constants.STREAM_OPERATIONS.DEFINITION._
import com.stratio.bus.messaging.MessageBuilder._

case class AddQueryMessageBuilder(sessionId: String) {
  val operation = ADD_QUERY.toLowerCase
  def build(streamName: String, query: String) = {
    builder.withOperation(operation)
      .withStreamName(streamName)
      .withRequest(query)
      .build()
  }

}
