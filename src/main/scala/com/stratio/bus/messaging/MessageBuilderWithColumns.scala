package com.stratio.bus.messaging

import java.util.List
import com.stratio.streaming.commons.messages.ColumnNameTypeValue
import scala.collection.JavaConversions._
import MessageBuilder._


case class MessageBuilderWithColumns(sessionId: String, operation: String) {
  def build(streamName: String, columns: List[ColumnNameType]) = {
    val columnNameTypeValueList = columns.toList.map(element => new ColumnNameTypeValue(element.columnName, element.columnType.getValue, null))
    builder.withColumns(columnNameTypeValueList)
      .withOperation(operation)
      .withStreamName(streamName)
      .withSessionId(sessionId)
      .build()
  }
}
