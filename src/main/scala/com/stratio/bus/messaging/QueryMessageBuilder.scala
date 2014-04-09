package com.stratio.bus.messaging

import com.stratio.streaming.commons.constants.STREAM_OPERATIONS.DEFINITION._
import com.stratio.bus.messaging.MessageBuilder._

case class QueryMessageBuilder(sessionId: String) {
  def build(streamName: String, query: String, operation: String) = {
    builder.withOperation(operation)
      .withStreamName(streamName)
      .withSessionId(sessionId)
      .withRequest(query)
      .build()
  }
}
