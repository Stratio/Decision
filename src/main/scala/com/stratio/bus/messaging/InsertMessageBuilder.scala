package com.stratio.bus.messaging

import java.util.List
import com.stratio.streaming.commons.messages.{StratioStreamingMessage, ColumnNameTypeValue}
import scala.collection.JavaConversions._
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS.MANIPULATION._
import com.stratio.bus.messaging.MessageBuilder._


case class InsertMessageBuilder(sessionId: String) {
  val operation = INSERT.toLowerCase

  def build(streamName: String, data: List[ColumnNameValue]) = {
    val columnNameTypeValueList = data.toList.map(element => new ColumnNameTypeValue(element.columnName, null, element.columnValue))
    builder.withColumns(columnNameTypeValueList)
      .withOperation(operation)
      .withStreamName(streamName)
      .withSessionId(sessionId)
      .build()
  }
}
