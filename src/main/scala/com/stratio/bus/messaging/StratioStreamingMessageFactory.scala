package com.stratio.bus.messaging

import com.stratio.streaming.commons.constants.STREAM_OPERATIONS.DEFINITION
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS.DEFINITION._
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS.ACTION._
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS.MANIPULATION._

object StratioStreamingMessageFactory {

  def getMessageBuilder(operation: String) = {
    operation.toUpperCase match {
      case DEFINITION.CREATE =>
    }
  }

}
