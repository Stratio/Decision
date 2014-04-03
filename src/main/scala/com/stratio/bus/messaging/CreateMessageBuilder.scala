package com.stratio.bus.messaging

import java.util.List
import com.stratio.streaming.commons.messages.{StratioStreamingMessage, ColumnNameTypeValue}
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS
import scala.collection.JavaConversions._


case class CreateMessageBuilder(sessionId: String) {
  val operation = STREAM_OPERATIONS.DEFINITION.CREATE.toLowerCase

  def build(streamName: String, columns: List[ColumnNameType]) = {
    val createStreamMessage = new StratioStreamingMessage
    val columnNameTypeValueList = columns.toList.map(element => new ColumnNameTypeValue(element.columnName, element.columnType.getValue, null))
    createStreamMessage.setColumns(columnNameTypeValueList)
    createStreamMessage.setOperation(operation)
    createStreamMessage.setStreamName(streamName)
    createStreamMessage.setRequest_id("" + System.currentTimeMillis)
    createStreamMessage.setSession_id(sessionId)
    createStreamMessage.setTimestamp(new java.lang.Long(System.currentTimeMillis))
    createStreamMessage
  }
}
