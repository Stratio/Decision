package com.stratio.bus

import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent._
import com.typesafe.config.ConfigFactory


class StratioBus
  extends IStratioBus {
    import StratioBus._

  def create(tableName: String, tableValues: Map[String, BusDataTypes.DataType]) = {
    val groupId = UUID.randomUUID().toString
    //Create topic
    KafkaTopicUtils.createTopic(zookeperCluster,tableName)
    //Create topic consumer
    val createTableConsumer = new KafkaConsumer(tableName, groupId, zookeperCluster, false)

    //Send message to the create table producer topic
    createTableProducer.send(s"table $tableName created")

    //Waiting for the Ack...
    try {
      val createTableTimeOut = config.getString("create.table.ack.timeout").toInt
      Await.result(createTableConsumer.read(messageCreatedOk), createTableTimeOut seconds)
    } catch {
      case e: TimeoutException => {
        val producer = new KafkaProducer(tableName, kafkaBroker)
        producer.send("ack")
      }
    }

    def messageCreatedOk(createdMessage: Array[Byte]) = {
      val messageContent = new String(createdMessage)
      println("Message  = " + messageContent + " consumed!!!!")
      createTableConsumer.close()
    }
  }

  def insert = ???

  def select = ???
}

object StratioBus {
  val config = ConfigFactory.load()
  val createTopicName = config.getString("create.table.topic.name")
  val brokerServer = config.getString("broker.server")
  val brokerIp = config.getString("broker.ip")
  val kafkaBroker = s"$brokerServer:$brokerIp"
  val zookeeperServer = config.getString("zookeeper.server")
  val zookeeperPort = config.getString("zookeeper.port")
  val zookeperCluster = s"$zookeeperServer:$zookeeperPort"

  lazy val createTableProducer = new KafkaProducer(createTopicName, kafkaBroker)

  def initializeTopics(topicName: String) {
    KafkaTopicUtils.createTopic(zookeperCluster,topicName)
  }
  
  def apply() = {
    initializeTopics(createTopicName)
    new StratioBus()
  }
}
