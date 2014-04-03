package com.stratio.bus

import com.stratio.streaming.commons.messages.StratioStreamingMessage
import org.apache.commons.lang.StringUtils
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS.DEFINITION._
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS.ACTION._
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS.MANIPULATION._

class StratioStreamingMessageBuilder {

  def buildMessage() = {
    val message = new StratioStreamingMessage
    val message = new StratioStreamingMessage
    message.setColumns(decodeColumns(operation, request))
    message.setRequest(request.trim)
    message.setOperation(operation)
    message.setStreamName(stream)
    message.setRequest_id("" + System.currentTimeMillis)
    message.setSession_id(sessionId)
    message.setTimestamp(new java.lang.Long(System.currentTimeMillis))
    message
  }

}
