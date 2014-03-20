package com.stratio.bus

import com.typesafe.config.ConfigFactory
import com.netflix.curator.retry.ExponentialBackoffRetry
import com.netflix.curator.framework.CuratorFrameworkFactory

class StratioBus
  extends IStratioBus {
  import StratioBus._

  def create(queryString: String) = stratioBusCreate.performSyncOperation(queryString)

  def insert(queryString: String) = stratioBusInsert.performAsyncOperation(queryString)

  def select(queryString: String) = stratioBusSelect.performSyncOperation(queryString)

  def alter(queryString: String) = stratioBusAlter.performAsyncOperation(queryString)

  def drop(queryString: String) = stratioBusDrop.performSyncOperation(queryString)

  def initialize() = {
    initializeTopic()
    this
  }
}

object StratioBus {
  val config = ConfigFactory.load()
  val streamingTopicName = config.getString("streaming.topic.name")
  val brokerServer = config.getString("broker.server")
  val brokerPort = config.getString("broker.port")
  val kafkaBroker = s"$brokerServer:$brokerPort"
  val zookeeperServer = config.getString("zookeeper.server")
  val zookeeperPort = config.getString("zookeeper.port")
  val zookeeperCluster = s"$zookeeperServer:$zookeeperPort"

  lazy val kafkaProducer = new KafkaProducer(streamingTopicName, kafkaBroker)

  val retryPolicy = new ExponentialBackoffRetry(1000, 3)
  lazy val zookeeperClient = CuratorFrameworkFactory.newClient(zookeeperCluster, retryPolicy)
  lazy val zookeeperConsumer = {
    zookeeperClient.start()
    ZookeeperConsumer(zookeeperClient)
  }

  lazy val stratioBusCreate = new BusSyncOperation(kafkaProducer, zookeeperConsumer, "create")
  lazy val stratioBusInsert = new BusAsyncOperation(kafkaProducer, "insert")
  lazy val stratioBusSelect = new BusSyncOperation(kafkaProducer, zookeeperConsumer, "select")
  lazy val stratioBusAlter = new BusAsyncOperation(kafkaProducer, "alter")
  lazy val stratioBusDrop = new BusSyncOperation(kafkaProducer, zookeeperConsumer, "drop")

  def initializeTopic() {
    KafkaTopicUtils.createTopic(zookeeperCluster, streamingTopicName)
  }
}
