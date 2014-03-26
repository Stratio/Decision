package com.stratio.bus

import scala.concurrent._
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._
import java.util.UUID
import org.slf4j.LoggerFactory
import com.stratio.streaming.commons.{Paths, ReplyCodes}
import com.google.gson.Gson
import com.stratio.streaming.commons.messages.StratioStreamingMessage
import com.stratio.bus.exception.StratioBusException
import com.stratio.bus.zookeeper.ZookeeperConsumer
import com.stratio.bus.kafka.KafkaProducer

case class BusSyncOperation(
  tableProducer: KafkaProducer,
  zookeeperConsumer: ZookeeperConsumer) {
  val config = ConfigFactory.load()
  val log = LoggerFactory.getLogger(getClass)
  val streamingAckTimeOut = config.getString("streaming.ack.timeout.in.seconds").toInt
  val ackErrorList = Map(ReplyCodes.KO_GENERAL_ERROR -> "Generic error",
    ReplyCodes.KO_PARSER_ERROR -> "Parser error",
    ReplyCodes.KO_LISTENER_ALREADY_EXISTS -> "Listener already exists",
    ReplyCodes.KO_QUERY_ALREADY_EXISTS -> "Query already exists",
    ReplyCodes.KO_STREAM_ALREADY_EXISTS -> "Stream already exists",
    ReplyCodes.KO_STREAM_DOES_NOT_EXIST -> "Stream does not exist",
    ReplyCodes.KO_COLUMN_ALREADY_EXISTS -> "Column already exists",
    ReplyCodes.KO_COLUMN_DOES_NOT_EXISTS -> "Column does not exist"
  )

  def performSyncOperation(message: StratioStreamingMessage) = {
    val zNodeUniqueId = UUID.randomUUID().toString
    addMessageToKafkaTopic(message, zNodeUniqueId)
    waitForTheStreamingResponse(message)
  }

  private def addMessageToKafkaTopic(message: StratioStreamingMessage, creationUniqueId: String) = {
    val kafkaMessage = new Gson().toJson(message)
    tableProducer.send(kafkaMessage, message.getOperation)
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
        log.error("Stratio Bus - Ack timeout expired for: "+message.getRequest)
        throw new StratioBusException("Ack timeout expired for: "+message.getRequest)
      }
    }
  }

  private def manageStreamingResponse(response: Option[String], message: StratioStreamingMessage) = {
    response.get match {
      case replyCode if isAnOkResponse(replyCode) => log.info("Stratio Bus - Ack received for: "+message.getRequest)
      case replyCode if isAnErrorResponse(replyCode) => {
        createLogError(replyCode, message.getRequest)
        val errorMessage = ackErrorList.get(response.get).get
        throw new StratioBusException("Error response received from StratioStreaming: "+errorMessage)
      }
      case _ => {
        log.info("Stratio Bus - ACK unknown response code received")
        throw new StratioBusException("Stratio Bus - ACK unknown response code received")
      }
    }
  }

  private def isAnOkResponse(replyCode: String) = replyCode == ReplyCodes.OK

  private def isAnErrorResponse(replyCode: String) = ackErrorList.contains(replyCode)

  private def createLogError(responseCode: String, queryString: String) = {
    log.error(s"Stratio Bus - [ACK_CODE,QUERY_STRING]: [$responseCode,$queryString]")
  }

  private def getOperationZNodeFullPath(message: StratioStreamingMessage) = {
    val zookeeperBasePath = Paths.ZK_BASE_PATH
    val operation = message.getOperation.toLowerCase()
    val uniqueId = message.getRequest_id
    val zookeeperPath = s"$zookeeperBasePath/$operation/$uniqueId"
    log.info(s"Stratio Bus - Waiting for zookeeper node response. Listen to the following path: $zookeeperPath")
    zookeeperPath
  }
}
