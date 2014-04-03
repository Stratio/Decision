package com.stratio.bus.messaging

import com.stratio.streaming.commons.messages.{ColumnNameTypeValue, StratioStreamingMessage}
import java.util.List

object MessageBuilder {

  def buildMessage(columns: List[ColumnNameTypeValue],
                    operation: String,
                    streamName: String,
                    sessionId: String) = {
    val createStreamMessage = new StratioStreamingMessage
    createStreamMessage.setColumns(columns)
    createStreamMessage.setOperation(operation)
    createStreamMessage.setStreamName(streamName)
    createStreamMessage.setRequest_id("" + System.currentTimeMillis)
    createStreamMessage.setSession_id(sessionId)
    createStreamMessage.setTimestamp(new java.lang.Long(System.currentTimeMillis))
    createStreamMessage
  }

}
