package com.stratio.bus

import scala.concurrent._
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._
import java.util.UUID
import org.slf4j.LoggerFactory
import com.stratio.streaming.commons.messages.StratioStreamingMessage
import com.stratio.streaming.commons.exceptions.StratioEngineOperationException
import com.stratio.bus.kafka.KafkaProducer
import com.stratio.bus.zookeeper.ZookeeperConsumer
import com.stratio.streaming.commons.constants.REPLY_CODES._
import com.stratio.streaming.commons.constants.STREAMING._

case class StreamingAPISyncOperation(
  kafkaProducer: KafkaProducer,
  zookeeperConsumer: ZookeeperConsumer)
  extends StreamingAPIOperation {

  def performSyncOperation(message: StratioStreamingMessage) = {
    val zNodeUniqueId = UUID.randomUUID().toString
    addMessageToKafkaTopic(message, zNodeUniqueId, kafkaProducer)
    waitForTheStreamingResponse(message)
  }

  private def waitForTheStreamingResponse(message: StratioStreamingMessage) = {
    val zNodeFullPath = getOperationZNodeFullPath(message)
    try {
      Await.result(zookeeperConsumer.readZNode(zNodeFullPath), streamingAckTimeOut seconds)
      val response = zookeeperConsumer.getZNodeData(zNodeFullPath)
      manageStreamingResponse(response, message)
      zookeeperConsumer.removeZNode(zNodeFullPath)
    } catch {
      case e: TimeoutException => {
        log.error("StratioAPI - Ack timeout expired for: "+message.getRequest)
        throw new StratioEngineOperationException("Acknowledge timeout expired"+message.getRequest)
      }
    }
  }

  private def manageStreamingResponse(response: Option[String], message: StratioStreamingMessage) = {
    val replyCode = getResponseCode(response.get)
    replyCode match {
      case Some(OK) => log.info("StratioEngine Ack received for: "+message.getRequest)
      case _ => {
        createLogError(response.get, message.getRequest)
        throw new StratioEngineOperationException("StratioEngine error: "+getReadableErrorFromCode(replyCode.get))
      }
    }
  }

  private def getResponseCode(response: String): Option[Integer] = {
    try {
      Some(new Integer(response))
    } catch {
      case ex: NumberFormatException => None
    }
  }

  private def createLogError(responseCode: String, queryString: String) = {
    log.error(s"StratioAPI - [ACK_CODE,QUERY_STRING]: [$responseCode,$queryString]")
  }

  private def getOperationZNodeFullPath(message: StratioStreamingMessage) = {
    val zookeeperBasePath = ZK_BASE_PATH
    val operation = message.getOperation.toLowerCase()
    val uniqueId = message.getRequest_id
    val zookeeperPath = s"$zookeeperBasePath/$operation/$uniqueId"
    log.info(s"StratioAPI - Waiting for zookeeper node response. Listen to the following path: $zookeeperPath")
    zookeeperPath
  }
}
