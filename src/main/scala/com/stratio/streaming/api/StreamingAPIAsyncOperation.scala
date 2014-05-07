/*
 * Copyright 2014 Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stratio.streaming.api

import com.google.gson.Gson
import com.stratio.streaming.commons.messages.StratioStreamingMessage
import com.stratio.streaming.kafka.KafkaProducer

case class StreamingAPIAsyncOperation(tableProducer: KafkaProducer) {
  def performAsyncOperation(message: StratioStreamingMessage) = {
    addMessageToKafkaTopic(message)
  }

  def addMessageToKafkaTopic(message: StratioStreamingMessage) = {
    val kafkaMessage = new Gson().toJson(message)
    val operation = message.getOperation
    tableProducer.send(kafkaMessage, operation)
  }
}
