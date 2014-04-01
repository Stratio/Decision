package com.stratio.bus

import com.typesafe.config.ConfigFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import com.stratio.streaming.commons.constants.CEPOperations._
import com.stratio.streaming.commons.messages.StratioStreamingMessage
import com.stratio.bus.zookeeper.ZookeeperConsumer
import com.stratio.bus.kafka.{KafkaTopicUtils, KafkaProducer}
import com.stratio.streaming.commons.constants.{CEPOperations, TopicNames}
import com.stratio.streaming.commons.constants.Paths._
import org.apache.curator.framework.api.{CuratorEvent, CuratorListener}
import org.apache.curator.framework.api.CuratorEventType._
import com.stratio.streaming.commons.exceptions.{StratioEngineStatusException, StratioStreamingException}

class StratioBus
  extends IStratioBus {
  import StratioBus._

  def send(message: StratioStreamingMessage) = {
    checkStreamingStatus()
    message.getOperation.toUpperCase match {
      case CEPOperations.CREATE | DROP | ALTER | LISTEN => syncOperation.performSyncOperation(message)
      case INSERT | ADD_QUERY | LIST => asyncOperation.performAsyncOperation(message)
      case _ => throw new StratioStreamingException("Unknown operation")
      //TODO IMPLEMENT METHODS
      //case SAVETO_DATACOLLECTOR => "SAVETO_DATACOLLECTOR"
      //case SAVETO_CASSANDRA => "SAVETO_CASSANDRA"
    }
  }

  def initialize() = {
    checkEphemeralNode()
    startEphemeralNodeWatch()
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
  var streamingUpAndRunning = false

  lazy val kafkaProducer = new KafkaProducer("stratio_streaming_requests", kafkaBroker)

  val retryPolicy = new ExponentialBackoffRetry(1000, 3)
  lazy val zookeeperClient = CuratorFrameworkFactory.newClient(zookeeperCluster, retryPolicy)
  lazy val zookeeperConsumer = {
    zookeeperClient.start()
    ZookeeperConsumer(zookeeperClient)
  }

  lazy val syncOperation = new BusSyncOperation(kafkaProducer, zookeeperConsumer)
  lazy val asyncOperation = new BusAsyncOperation(kafkaProducer)

  def checkEphemeralNode() {
    val ephemeralNodePath = ZK_EPHEMERAL_NODE_PATH
    if (!zookeeperConsumer.zNodeExists(ephemeralNodePath))
      throw new StratioEngineStatusException("Stratio streaming is down")
  }

  def startEphemeralNodeWatch() {
    zookeeperClient.checkExists().watched().forPath(ZK_EPHEMERAL_NODE_PATH)
    addListener()
  }

  def initializeTopic() {
    KafkaTopicUtils.createTopicIfNotExists(zookeeperCluster, streamingTopicName)
  }

  def checkStreamingStatus() {
     if (!streamingUpAndRunning) throw new StratioEngineStatusException("Stratio streaming is down")
  }

  def addListener() = {
    zookeeperClient.getCuratorListenable().addListener(new CuratorListener() {
      def eventReceived(client: CuratorFramework, event: CuratorEvent) = {
        event.getType() match {
          case WATCHED => {
            zookeeperClient.checkExists().watched().forPath(ZK_EPHEMERAL_NODE_PATH)
            zookeeperConsumer.zNodeExists(ZK_EPHEMERAL_NODE_PATH) match {
              case true => streamingUpAndRunning = true
              case false => streamingUpAndRunning = false
            }
          }
        }
      }
    })
  }
}
