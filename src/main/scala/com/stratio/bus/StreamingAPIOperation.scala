package com.stratio.bus

import com.stratio.streaming.commons.messages.StratioStreamingMessage
import com.google.gson.Gson
import com.stratio.streaming.commons.constants.STREAMING._
import scala.concurrent.duration._
import scala.concurrent._
import com.stratio.bus.kafka.KafkaProducer
import com.stratio.streaming.commons.exceptions.StratioEngineOperationException
import com.stratio.bus.zookeeper.ZookeeperConsumer
import org.slf4j.LoggerFactory

class StreamingAPIOperation
  extends StratioStreamingAPIConfig{
  protected val log = LoggerFactory.getLogger(getClass)
  protected val streamingAckTimeOut = config.getString("streaming.ack.timeout.in.seconds").toInt

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
      Await.result(zookeeperConsumer.readZNode(zNodeFullPath), streamingAckTimeOut seconds)
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
