package com.stratio.streaming.messaging

import com.stratio.streaming.messaging.MessageBuilder._

case class StreamMessageBuilder(sessionId: String) {
  def build(streamName: String, operation: String) = {
    builder.withOperation(operation)
      .withStreamName(streamName)
      .withSessionId(sessionId)
      .build()
  }
}
