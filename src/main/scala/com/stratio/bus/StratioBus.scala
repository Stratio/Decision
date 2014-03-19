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
    initializeTopics()
    this
  }
}

object StratioBus {
  val config = ConfigFactory.load()
  val createTopicName = config.getString("create.table.topic.name")
  val insertTopicName = config.getString("insert.table.topic.name")
  val selectTopicName = config.getString("select.table.topic.name")
  val alterTopicName = config.getString("alter.table.topic.name")
  val dropTopicName = config.getString("drop.table.topic.name")
  val brokerServer = config.getString("broker.server")
  val brokerPort = config.getString("broker.port")
  val kafkaBroker = s"$brokerServer:$brokerPort"
  val zookeeperServer = config.getString("zookeeper.server")
  val zookeeperPort = config.getString("zookeeper.port")
  val zookeeperCluster = s"$zookeeperServer:$zookeeperPort"

  lazy val createTableProducer = new KafkaProducer(createTopicName, kafkaBroker)
  lazy val insertTableProducer = new KafkaProducer(insertTopicName, kafkaBroker)
  lazy val selectTableProducer = new KafkaProducer(selectTopicName, kafkaBroker)
  lazy val alterTableProducer = new KafkaProducer(alterTopicName, kafkaBroker)
  lazy val dropTableProducer = new KafkaProducer(dropTopicName, kafkaBroker)

  val retryPolicy = new ExponentialBackoffRetry(1000, 3)
  lazy val zookeeperClient = CuratorFrameworkFactory.newClient(zookeeperCluster, retryPolicy)
  lazy val zookeeperConsumer = {
    zookeeperClient.start()
    ZookeeperConsumer(zookeeperClient)
  }

  lazy val stratioBusCreate = new BusSyncOperation(createTableProducer, zookeeperConsumer, "create")
  lazy val stratioBusInsert = new BusAsyncOperation(insertTableProducer)
  lazy val stratioBusSelect = new BusSyncOperation(selectTableProducer, zookeeperConsumer, "select")
  lazy val stratioBusAlter = new BusAsyncOperation(alterTableProducer)
  lazy val stratioBusDrop = new BusSyncOperation(dropTableProducer, zookeeperConsumer, "drop")

  def initializeTopics() {
    KafkaTopicUtils.createTopic(zookeeperCluster, createTopicName)
    KafkaTopicUtils.createTopic(zookeeperCluster, insertTopicName)
    KafkaTopicUtils.createTopic(zookeeperCluster, selectTopicName)
    KafkaTopicUtils.createTopic(zookeeperCluster, alterTopicName)
    KafkaTopicUtils.createTopic(zookeeperCluster, dropTopicName)
  }
}
