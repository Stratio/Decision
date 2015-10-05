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

import com.google.gson.Gson
import com.stratio.decision.api.kafka.KafkaProducer
import com.stratio.decision.api.zookeeper.ZookeeperConsumer
import com.stratio.decision.commons.constants.STREAMING._
import com.stratio.decision.commons.exceptions.StratioEngineOperationException
import com.stratio.decision.commons.messages.StratioStreamingMessage
import org.slf4j.LoggerFactory

import scala.concurrent._
import scala.concurrent.duration._

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
    message: StratioStreamingMessage,
    ackTimeOutInMs: Int) = {
    val zNodeFullPath = getOperationZNodeFullPath(
      message.getOperation.toLowerCase,
      message.getRequest_id)
    try {
      Await.result(zookeeperConsumer.readZNode(zNodeFullPath), ackTimeOutInMs milliseconds)
      val response = zookeeperConsumer.getZNodeData(zNodeFullPath)
      zookeeperConsumer.removeZNode(zNodeFullPath)
      response.get
    } catch {
      case e: TimeoutException => {
        log.error("Ack timeout expired for: " + message.getRequest)
        throw new StratioEngineOperationException("Acknowledge timeout expired" + message.getRequest)
      }
    }
  }
}
