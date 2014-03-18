package com.stratio.bus

import scala.concurrent._
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._
import java.util.UUID
import com.stratio.bus.utils.JsonUtils
import com.stratio.bus.StreamingAckValues._
import com.stratio.bus.KafkaProducer
import com.stratio.bus.ZookeeperConsumer
import scala.Some

case class BusSyncOperation(
  tableProducer: KafkaProducer,
  zookeeperConsumer: ZookeeperConsumer,
  operation: String) {
  val config = ConfigFactory.load()
  val streamingAckTimeOut = config.getString("streaming.ack.timeout.in.seconds").toInt

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
      val zNodeFullPath = getOperationZNodeFullPath(zNodeUniqueId, operation)
      Await.result(zookeeperConsumer.readZNode(zNodeFullPath), streamingAckTimeOut seconds)
      val response = zookeeperConsumer.getZNodeData(zNodeFullPath)
      println("The response has been: "+response.get)
      response.get match {
        case StreamingAckValues(AckOk) => println("ACK OK!!")
        case StreamingAckValues(AckError) => println("ACK ERROR!!!")
        case _ => println("I HAVE NO IDEA WHAT TO DO WITH THIS :(")
      }
      //TODO define response (json, exceptions....)
    } catch {
      case e: TimeoutException => {
        println("TIME OUT")
        //TODO insert error into error-topic ???
      }
    }
  }

  private def getOperationZNodeFullPath(uniqueId: String, operation: String) = {
    val zookeeperPath = config.getString("zookeeper.listener.base.path")
    s"$zookeeperPath/$operation/$uniqueId"
  }
}
