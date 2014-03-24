package com.stratio.bus

import scala.concurrent._
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._
import java.util.UUID
import com.stratio.bus.utils.JsonUtils
import org.slf4j.LoggerFactory
import com.stratio.streaming.commons.{Paths, ReplyCodes}
import com.google.gson.Gson
import com.stratio.streaming.commons.messages.StratioStreamingMessage

case class BusSyncOperation(
  tableProducer: KafkaProducer,
  zookeeperConsumer: ZookeeperConsumer) {
  val config = ConfigFactory.load()
  val log = LoggerFactory.getLogger(getClass)
  val streamingAckTimeOut = config.getString("streaming.ack.timeout.in.seconds").toInt
  val ackErrorList = List(
    ReplyCodes.KO_GENERAL_ERROR,
    ReplyCodes.KO_PARSER_ERROR,
    ReplyCodes.KO_LISTENER_ALREADY_EXISTS,
    ReplyCodes.KO_QUERY_ALREADY_EXISTS,
    ReplyCodes.KO_STREAM_ALREADY_EXISTS,
    ReplyCodes.KO_STREAM_DOES_NOT_EXIST,
    ReplyCodes.KO_COLUMN_ALREADY_EXISTS,
    ReplyCodes.KO_COLUMN_DOES_NOT_EXISTS
  )

  def performSyncOperation(message: StratioStreamingMessage) = {
    val zNodeUniqueId = UUID.randomUUID().toString
    addMessageToKafkaTopic(message, zNodeUniqueId)
    waitForTheStreamingResponse(zNodeUniqueId, message)
  }

  private def addMessageToKafkaTopic(message: StratioStreamingMessage, creationUniqueId: String) = {
    val kafkaMessage = JsonUtils.appendElementsToJsonString(new Gson().toJson(message), Map("zNodeId" -> creationUniqueId))
    tableProducer.send(kafkaMessage, message.getOperation)
  }
  private def waitForTheStreamingResponse(zNodeUniqueId: String, message: StratioStreamingMessage) = {
    val zNodeFullPath = getOperationZNodeFullPath(zNodeUniqueId, message)
    try {
      Await.result(zookeeperConsumer.readZNode(zNodeFullPath), streamingAckTimeOut seconds)
      val response = zookeeperConsumer.getZNodeData(zNodeFullPath)
      manageStreamingResponse(response, message)
      zookeeperConsumer.removeZNode(zNodeFullPath)
    } catch {
      case e: TimeoutException => {
        log.error("Stratio Bus - Ack from zookeeper timeout expired for: "+message.getRequest)
        //TODO insert error into error-topic ???
      }
    }
  }

  private def manageStreamingResponse(response: Option[String], message: StratioStreamingMessage) = {
    //TODO define response (json, exceptions....)
    response.get match {
      case replyCode if isAnOkResponse(replyCode) => log.info("Stratio Bus - Ack received for: "+message.getRequest)
      case replyCode if isAnErrorResponse(replyCode) => createLogError(replyCode, message.getRequest)
      case _ => log.info("Stratio Bus - I have no idea what to do with this")
    }
  }

  private def isAnOkResponse(replyCode: String) = replyCode == ReplyCodes.OK

  private def isAnErrorResponse(replyCode: String) = ackErrorList.contains(replyCode)

  private def createLogError(responseCode: String, queryString: String) = {
    log.error(s"Stratio Bus - [ACK_CODE,QUERY_STRING]: [$responseCode,$queryString]")
  }

  private def getOperationZNodeFullPath(uniqueId: String, message: StratioStreamingMessage) = {
    val zookeeperPath = Paths.ZK_BASE_PATH
    val operation = message.getOperation
    s"$zookeeperPath/$operation/$uniqueId"
  }
}
