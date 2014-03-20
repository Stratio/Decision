package com.stratio.bus

import scala.concurrent._
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._
import java.util.UUID
import com.stratio.bus.utils.JsonUtils
import org.slf4j.LoggerFactory
import com.stratio.streaming.commons.{Paths, ReplyCodes}

case class BusSyncOperation(
  tableProducer: KafkaProducer,
  zookeeperConsumer: ZookeeperConsumer,
  operation: String) {
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

  def performSyncOperation(queryString: String) = {
    val zNodeUniqueId = UUID.randomUUID().toString
    addMessageToKafkaTopic(queryString, zNodeUniqueId)
    waitForTheStreamingResponse(zNodeUniqueId, queryString)
  }

  private def addMessageToKafkaTopic(queryString: String, creationUniqueId: String) = {
    val message = JsonUtils.appendElementsToJsonString(queryString, Map("zNodeId" -> creationUniqueId))
    tableProducer.send(message, operation)
  }
  private def waitForTheStreamingResponse(zNodeUniqueId: String, queryString: String) = {
    val zNodeFullPath = getOperationZNodeFullPath(zNodeUniqueId, operation)
    try {
      Await.result(zookeeperConsumer.readZNode(zNodeFullPath), streamingAckTimeOut seconds)
      val response = zookeeperConsumer.getZNodeData(zNodeFullPath)
      manageStreamingResponse(response, queryString)
      zookeeperConsumer.removeZNode(zNodeFullPath)
    } catch {
      case e: TimeoutException => {
        log.error("Stratio Bus - Ack from zookeeper timeout expired for: "+queryString)
        //TODO insert error into error-topic ???
      }
    }
  }

  private def manageStreamingResponse(response: Option[String], queryString: String) = {
    //TODO define response (json, exceptions....)
    val okCode = ReplyCodes.OK
    response.get match {
      case replyCode if isAnOkResponse(replyCode) => log.info("Stratio Bus - Ack received for: "+queryString)
      case replyCode if isAnErrorResponse(replyCode) => createLogError(replyCode, queryString)
      case _ => log.info("Stratio Bus - I have no idea what to do with this")
    }
  }

  private def isAnOkResponse(replyCode: String) = replyCode == ReplyCodes.KO_GENERAL_ERROR

  private def isAnErrorResponse(replyCode: String) = ackErrorList.contains(replyCode)

  private def createLogError(responseCode: String, queryString: String) = {
    log.error(s"Stratio Bus - [ACK_CODE,QUERY_STRING]: [$responseCode,$queryString]")
  }

  private def getOperationZNodeFullPath(uniqueId: String, operation: String) = {
    val zookeeperPath = Paths.ZK_BASE_PATH
    s"$zookeeperPath/$operation/$uniqueId"
  }
}
