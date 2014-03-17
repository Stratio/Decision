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
    val creationUniqueId = UUID.randomUUID().toString
    createMessageInTheCreationTopic(queryString, creationUniqueId)
    waitForTheStreamingResponse(creationUniqueId)
  }

  def createMessageInTheCreationTopic(queryString: String, creationUniqueId: String) = {
    val message = JsonUtils.appendElementsToJson(queryString, Map("zNodeId" -> creationUniqueId))
    createTableProducer.send(message)
  }
  def waitForTheStreamingResponse(creationUniqueId: String) = {
    try {
      val createTableTimeOut = config.getString("create.table.ack.timeout.in.seconds").toInt
      Await.result(zookeeperConsumer.readZNode(creationUniqueId, "create"), createTableTimeOut seconds)
      println("ACK RECEIVED!!!!")
      //TODO define response (json, exceptions....)
    } catch {
      case e: TimeoutException => {
        println("TIME OUT")
        //TODO insert error into error-topic ???
      }
    }
  }
}
