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
package com.stratio.decision.api

import com.stratio.decision.api.kafka.KafkaProducer
import com.stratio.decision.commons.avro.{ColumnType, InsertMessage}
import com.stratio.decision.commons.messages.{ColumnNameTypeValue, StratioStreamingMessage}
import collection.JavaConversions._

class StreamingAPIAsyncOperation(tableProducer: KafkaProducer) {

  def performAsyncOperation(message: StratioStreamingMessage, topicName:String) : Unit = {
    addAvroMessageToKafkaTopic(message, topicName)
  }

  def performAsyncOperation(message: StratioStreamingMessage) : Unit = {
    performAsyncOperation(message, null)
  }

  private def addAvroMessageToKafkaTopic(message: StratioStreamingMessage, topicName:String) = {
    val avroKafkaMessage : InsertMessage = convertMessage(message)
    tableProducer.sendAvro(avroKafkaMessage, message.getOperation, topicName)
  }

  private def convertMessage(stratioStreamingMessage : StratioStreamingMessage) : InsertMessage = {

    var columns = new java.util.ArrayList[com.stratio.decision.commons.avro.ColumnType]();
    var c : ColumnType = null

    for (messageColumn : ColumnNameTypeValue <- stratioStreamingMessage.getColumns){

      if (messageColumn.getValue != null) {
        c = new ColumnType(messageColumn.getColumn, messageColumn.getValue.toString, messageColumn.getValue.getClass
          .getName)
      } else {

        c = new ColumnType(messageColumn.getColumn, null, null);
      }
      columns.add(c)
    }

    new InsertMessage(stratioStreamingMessage.getOperation, stratioStreamingMessage.getStreamName,
      stratioStreamingMessage.getSession_id, null, columns, null)
  }

}
