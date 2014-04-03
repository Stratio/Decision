package com.stratio.bus.messaging

import com.stratio.streaming.commons.constants.STREAM_OPERATIONS.DEFINITION._
import com.stratio.bus.messaging.MessageBuilder._

case class DropMessageBuilder(sessionId: String) {
  val operation = DROP.toLowerCase
  def build(streamName: String) = {
    builder.withOperation(operation)
      .withStreamName(streamName)
      .withSessionId(sessionId)
      .build()
  }
}
