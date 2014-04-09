package com.stratio.bus.messaging

import com.stratio.bus.messaging.MessageBuilder._

case class StreamMessageBuilder(sessionId: String) {
  def build(streamName: String, operation: String) = {
    builder.withOperation(operation)
      .withStreamName(streamName)
      .withSessionId(sessionId)
      .build()
  }
}
