/*
 * Copyright 2014 Stratio Big Data, S.L.
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

import com.stratio.streaming.commons.messages.StratioStreamingMessage
import com.google.gson.Gson
import com.stratio.streaming.commons.constants.STREAMING._
import scala.concurrent.duration._
import scala.concurrent._
import com.stratio.streaming.kafka.KafkaProducer
import com.stratio.streaming.commons.exceptions.StratioEngineOperationException
import com.stratio.streaming.zookeeper.ZookeeperConsumer
import org.slf4j.LoggerFactory

class StreamingAPIOperation
  extends StratioStreamingAPIConfig {
  protected val log = LoggerFactory.getLogger(getClass)

  protected def addMessageToKafkaTopic(message: StratioStreamingMessage,
                                     creationUniqueId: String,
                                       tableProducer: KafkaProducer) = {
    val kafkaMessage = new Gson().toJson(message)
    tableProducer.send(kafkaMessage, message.getOperation)
  }

  protected def getOperationZNodeFullPath(operation: String, uniqueId: String) = {
    val zookeeperBasePath = ZK_BASE_PATH
    val zookeeperPath = s"$zookeeperBasePath/$operation/$uniqueId"
    zookeeperPath
  }

  protected def waitForTheStreamingResponse(zookeeperConsumer: ZookeeperConsumer,
                                  message: StratioStreamingMessage) = {
    val zNodeFullPath = getOperationZNodeFullPath(
      message.getOperation.toLowerCase,
      message.getRequest_id)
    try {
      Await.result(zookeeperConsumer.readZNode(zNodeFullPath), streamingAckTimeOutInSeconds seconds)
      val response = zookeeperConsumer.getZNodeData(zNodeFullPath)
      zookeeperConsumer.removeZNode(zNodeFullPath)
      response.get
    } catch {
      case e: TimeoutException => {
        log.error("StratioAPI - Ack timeout expired for: "+message.getRequest)
        throw new StratioEngineOperationException("Acknowledge timeout expired"+message.getRequest)
      }
    }
  }
}
