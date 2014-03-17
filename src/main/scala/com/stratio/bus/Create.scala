package com.stratio.bus

import scala.concurrent._
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._
import java.util.UUID
import com.stratio.bus.utils.JsonUtils

case class Create(
  createTableProducer: KafkaProducer,
  zookeeperConsumer: ZookeeperConsumer) {
  val config = ConfigFactory.load()

  def create(queryString: String) = {
    val zNodeUniqueId = UUID.randomUUID().toString
    createMessageInTheCreationTopic(queryString, zNodeUniqueId)
    waitForTheStreamingResponse(zNodeUniqueId)
  }

  def createMessageInTheCreationTopic(queryString: String, creationUniqueId: String) = {
    val message = JsonUtils.appendElementsToJsonString(queryString, Map("zNodeId" -> creationUniqueId))
    createTableProducer.send(message)
  }
  
  def waitForTheStreamingResponse(zNodeUniqueId: String) = {
    try {
      val createTableTimeOut = config.getString("create.table.ack.timeout.in.seconds").toInt
      val creationFullPath = getCreationZNodeFullPath(zNodeUniqueId, "create")
      Await.result(zookeeperConsumer.readZNode(creationFullPath), createTableTimeOut seconds)
      val response = zookeeperConsumer.getZNodeData(creationFullPath)
      println("The response has been: "+response)
      //TODO define response (json, exceptions....)
    } catch {
      case e: TimeoutException => {
        println("TIME OUT")
        //TODO insert error into error-topic ???
      }
    }
  }

  //TODO Extract????
  def getCreationZNodeFullPath(uniqueId: String, operation: String) = {
    val zookeeperPath = config.getString("zookeeper.listener.path")
    s"$zookeeperPath/$operation/$uniqueId"
  }
}
