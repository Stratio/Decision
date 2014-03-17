package com.stratio.bus

import scala.concurrent._
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._
import java.util.UUID

case class Create(
  createTableProducer: KafkaProducer,
  zookeeperConsumer: ZookeeperConsumer) {
  val config = ConfigFactory.load()

  def create(queryString: String) = {
    createMessageInTheCreationTopic(queryString)
    waitForTheStreamingResponse()
  }

  def createMessageInTheCreationTopic(queryString: String) = createTableProducer.send(queryString)

  def waitForTheStreamingResponse() = {
    try {
      val creationUniqueId = UUID.randomUUID().toString
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
