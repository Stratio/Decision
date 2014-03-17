package com.stratio.bus

import scala.concurrent._
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._

case class Create(
  createTableProducer: KafkaProducer,
  zookeeperConsumer: ZookeeperConsumer) {
  val config = ConfigFactory.load()

  def create(tableName: String, queryString: String) = {
    createMessageInTheCreationTopic(queryString)
    waitForTheStreamingResponse(tableName)
  }

  def createMessageInTheCreationTopic(queryString: String) = createTableProducer.send(queryString)

  def waitForTheStreamingResponse(tableName: String) = {
    try {
      val createTableTimeOut = config.getString("create.table.ack.timeout.in.seconds").toInt
      Await.result(zookeeperConsumer.readZNode(tableName), createTableTimeOut seconds)
      println("ACK RECEIVED!!!!")
    } catch {
      case e: TimeoutException => {
        println("TIME OUT")
        //TODO insert error into error-topic ???
      }
    }
  }
}
