package com.stratio.bus.messaging

import java.util.List
import com.stratio.streaming.commons.messages.{StratioStreamingMessage, ColumnNameTypeValue}
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS
import scala.collection.JavaConversions._


case class CreateAndAlterMessageBuilder(sessionId: String, operation: String) {
  def build(streamName: String, columns: List[ColumnNameType]) = {
    val columnNameTypeValueList = columns.toList.map(element => new ColumnNameTypeValue(element.columnName, element.columnType.getValue, null))
    MessageBuilder.buildMessage(columnNameTypeValueList, operation, streamName, sessionId)
  }
}
