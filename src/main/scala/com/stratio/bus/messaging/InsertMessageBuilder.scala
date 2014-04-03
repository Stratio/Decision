package com.stratio.bus.messaging

import java.util.List
import com.stratio.streaming.commons.messages.{StratioStreamingMessage, ColumnNameTypeValue}
import scala.collection.JavaConversions._
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS.MANIPULATION._


case class InsertMessageBuilder(sessionId: String) {
  val operation = INSERT.toLowerCase

  def build(streamName: String, data: List[ColumnNameValue]) = {
    val columnNameTypeValueList = data.toList.map(element => new ColumnNameTypeValue(element.columnName, null, element.columnValue))
    val createStreamMessage = new StratioStreamingMessage
    createStreamMessage.setColumns(columnNameTypeValueList)
    createStreamMessage.setOperation(operation)
    createStreamMessage.setStreamName(streamName)
    createStreamMessage.setRequest_id("" + System.currentTimeMillis)
    createStreamMessage.setSession_id(sessionId)
    createStreamMessage.setTimestamp(new java.lang.Long(System.currentTimeMillis))
    createStreamMessage
  }
}
