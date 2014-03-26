package com.stratio.bus

import com.typesafe.config.ConfigFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.framework.CuratorFrameworkFactory
import com.stratio.streaming.commons.TopicNames
import com.stratio.streaming.commons.constants.CEPOperations._
import com.stratio.streaming.commons.messages.StratioStreamingMessage
import com.stratio.bus.exception.StratioBusException

class StratioBus
  extends IStratioBus {
  import StratioBus._

  def send(message: StratioStreamingMessage) = {
    message.getOperation.toUpperCase match {
      case CREATE | DROP | ALTER | INSERT | LISTEN => syncOperation.performSyncOperation(message)
      case ADD_QUERY | LIST => asyncOperation.performAsyncOperation(message)
      case _ => throw new StratioBusException("Stratio Bus - Unknown operation")
      //TODO IMPLEMENT METHODS
      //case SAVETO_DATACOLLECTOR => "SAVETO_DATACOLLECTOR"
      //case SAVETO_CASSANDRA => "SAVETO_CASSANDRA"
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

  lazy val kafkaProducer = new KafkaProducer("stratio_streaming_requests", kafkaBroker)

  val retryPolicy = new ExponentialBackoffRetry(1000, 3)
  lazy val zookeeperClient = CuratorFrameworkFactory.newClient(zookeeperCluster, retryPolicy)
  lazy val zookeeperConsumer = {
    zookeeperClient.start()
    ZookeeperConsumer(zookeeperClient)
  }

  lazy val syncOperation = new BusSyncOperation(kafkaProducer, zookeeperConsumer)
  lazy val asyncOperation = new BusAsyncOperation(kafkaProducer)

  def initializeTopic() {
    KafkaTopicUtils.createTopicIfNotExists(zookeeperCluster, streamingTopicName)
  }
}
