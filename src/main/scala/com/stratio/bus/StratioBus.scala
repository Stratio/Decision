package com.stratio.bus

import scala.concurrent.duration._
import scala.concurrent._
import com.typesafe.config.ConfigFactory
import com.netflix.curator.retry.ExponentialBackoffRetry
import com.netflix.curator.framework.CuratorFrameworkFactory


class StratioBus
  extends IStratioBus {
  import StratioBus._

  def create(tableName: String, tableValues: Map[String, BusDataTypes.DataType]) = {
    createMessageInTheCreationTopic(tableName)
    waitForTheStreamingResponse(tableName)
  }

  def createMessageInTheCreationTopic(tableName: String) = createTableProducer.send(s"table $tableName created")

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
  val zookeeperCluster = s"$zookeeperServer:$zookeeperPort"

  lazy val createTableProducer = new KafkaProducer(createTopicName, kafkaBroker)
  val retryPolicy = new ExponentialBackoffRetry(1000, 3)
  val zookeeperClient = CuratorFrameworkFactory.newClient(zookeeperCluster, retryPolicy)
  lazy val zookeeperConsumer = {
    zookeeperClient.start()
    ZookeeperConsumer(zookeeperClient)
  }

  def initializeTopics(topicName: String) {
    KafkaTopicUtils.createTopic(zookeeperCluster, topicName)
  }
  
  def apply() = {
    initializeTopics(createTopicName)
    new StratioBus()
  }
}
