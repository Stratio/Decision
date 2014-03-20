package com.stratio.bus

import scala.concurrent._
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._
import java.util.UUID
import com.stratio.bus.utils.JsonUtils
import com.stratio.bus.StreamingAckValues._
import org.slf4j.LoggerFactory

case class BusSyncOperation(
  tableProducer: KafkaProducer,
  zookeeperConsumer: ZookeeperConsumer,
  operation: String) {
  val config = ConfigFactory.load()
  val log = LoggerFactory.getLogger(getClass)
  val streamingAckTimeOut = config.getString("streaming.ack.timeout.in.seconds").toInt

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
      response.get match {
        case StreamingAckValues(AckOk) => log.info("Stratio Bus - Ack received for: "+queryString)
        case StreamingAckValues(AckError) => log.error("Stratio Bus - Ack error [ACK_CODE,QUERY_STRING]: ["+AckError+","+queryString+"]")
        case _ => log.info("Stratio Bus - I have no idea what to do with this")
      }
      //TODO define response (json, exceptions....)
      zookeeperConsumer.removeZNode(zNodeFullPath)
    } catch {
      case e: TimeoutException => {
        log.error("Stratio Bus - Ack from zookeeper timeout expired for: "+queryString)
        //TODO insert error into error-topic ???
      }
    }
  }

  private def getOperationZNodeFullPath(uniqueId: String, operation: String) = {
    val zookeeperPath = config.getString("zookeeper.listener.base.path")
    s"$zookeeperPath/$operation/$uniqueId"
  }
}
