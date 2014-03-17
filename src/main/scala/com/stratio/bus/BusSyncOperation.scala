package com.stratio.bus

import scala.concurrent._
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._
import java.util.UUID
import com.stratio.bus.utils.JsonUtils

case class BusSyncOperation(
  tableProducer: KafkaProducer,
  zookeeperConsumer: ZookeeperConsumer,
  operation: String) {
  val config = ConfigFactory.load()

  def performSyncOperation(queryString: String) = {
    val zNodeUniqueId = UUID.randomUUID().toString
    addMessageToKafkaTopic(queryString, zNodeUniqueId)
    waitForTheStreamingResponse(zNodeUniqueId)
  }

  private def addMessageToKafkaTopic(queryString: String, creationUniqueId: String) = {
    val message = JsonUtils.appendElementsToJsonString(queryString, Map("zNodeId" -> creationUniqueId))
    tableProducer.send(message)
  }
  
  private def waitForTheStreamingResponse(zNodeUniqueId: String) = {
    try {
      val streamingAckTimeOut = config.getString("streaming.ack.timeout.in.seconds").toInt
      val zNodeFullPath = getOperationZNodeFullPath(zNodeUniqueId, operation)
      Await.result(zookeeperConsumer.readZNode(zNodeFullPath), streamingAckTimeOut seconds)
      val response = zookeeperConsumer.getZNodeData(zNodeFullPath)
      println("The response has been: "+response)
      //TODO define response (json, exceptions....)
    } catch {
      case e: TimeoutException => {
        println("TIME OUT")
        //TODO insert error into error-topic ???
      }
    }
  }

  private def getOperationZNodeFullPath(uniqueId: String, operation: String) = {
    val zookeeperPath = config.getString("zookeeper.listener.path")
    s"$zookeeperPath/$operation/$uniqueId"
  }
}
