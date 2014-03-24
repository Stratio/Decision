package com.stratio.bus

import com.typesafe.config.ConfigFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.framework.CuratorFrameworkFactory
import com.stratio.streaming.commons.TopicNames
import com.stratio.streaming.commons.constants.CEPOperations._
import com.stratio.streaming.commons.messages.StratioStreamingMessage

class StratioBus
  extends IStratioBus {
  import StratioBus._

  def send(message: StratioStreamingMessage) = {
    println("BUS OPERATION: "+message.getOperation)
    message.getOperation.toUpperCase match {
      case CREATE => stratioBusCreate.performSyncOperation(message)
      case ADD_QUERY => stratioBusSelect.performSyncOperation(message)
      case DROP => stratioBusDrop.performSyncOperation(message)
      case ALTER => stratioBusAlter.performAsyncOperation(message)
      case INSERT => stratioBusInsert.performAsyncOperation(message)
      //TODO IMPLEMENT METHODS
      //case LIST => "LIST"
      //case LISTEN => "LISTEN"
      //case SAVETO_DATACOLLECTOR => "SAVETO_DATACOLLECTOR"
      //case SAVETO_CASSANDRA => "SAVETO_CASSANDRA"
      //TODO CREATE EXCEPTION
      //case _ => throw new StratioBusException();
    }
  }

  def initialize() = {
    initializeTopic()
    this
  }
}

object StratioBus {
  val config = ConfigFactory.load()
  val streamingTopicName = TopicNames.STREAMING_REQUESTS_TOPIC
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

  lazy val stratioBusCreate = new BusSyncOperation(kafkaProducer, zookeeperConsumer)
  lazy val stratioBusInsert = new BusAsyncOperation(kafkaProducer)
  lazy val stratioBusSelect = new BusSyncOperation(kafkaProducer, zookeeperConsumer)
  lazy val stratioBusAlter = new BusAsyncOperation(kafkaProducer)
  lazy val stratioBusDrop = new BusSyncOperation(kafkaProducer, zookeeperConsumer)

  def initializeTopic() {
    KafkaTopicUtils.createTopic(zookeeperCluster, streamingTopicName)
  }
}
