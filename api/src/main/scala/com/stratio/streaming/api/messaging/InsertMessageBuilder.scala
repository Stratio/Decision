/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.streaming.messaging

import java.util.List
import com.stratio.streaming.commons.messages.{ StratioStreamingMessage, ColumnNameTypeValue }
import scala.collection.JavaConversions._
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS.MANIPULATION._
import com.stratio.streaming.api.messaging.MessageBuilder._
import com.stratio.streaming.api.messaging.ColumnNameValue

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
