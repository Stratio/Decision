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
package com.stratio.streaming.api

import com.stratio.streaming.commons.streams.StratioStream
import com.stratio.streaming.commons.messages.StratioStreamingMessage
import scala.collection.JavaConversions._
import java.util.UUID
import com.stratio.streaming.kafka.KafkaProducer
import com.stratio.streaming.zookeeper.ZookeeperConsumer
import com.stratio.streaming.utils.StreamsParser
import java.util.List
import scala.collection.immutable

class StreamingAPIListOperation(kafkaProducer: KafkaProducer,
                             zookeeperConsumer: ZookeeperConsumer,
                                 ackTimeOutInMs: Int)
  extends StreamingAPIOperation {

  def getListStreams(message: StratioStreamingMessage): List[StratioStream] = {
    val zNodeUniqueId = UUID.randomUUID().toString
    addMessageToKafkaTopic(message, zNodeUniqueId, kafkaProducer)
    val jsonStreamingResponse = waitForTheStreamingResponse(zookeeperConsumer, message, ackTimeOutInMs)
    val parsedList = StreamsParser.parse(jsonStreamingResponse)
    parsedList
  }
}
