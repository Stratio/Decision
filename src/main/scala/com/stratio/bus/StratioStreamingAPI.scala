package com.stratio.bus

import com.typesafe.config.ConfigFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import com.stratio.streaming.commons.constants.CEPOperations._
import com.stratio.streaming.commons.messages.StratioStreamingMessage
import com.stratio.bus.zookeeper.ZookeeperConsumer
import com.stratio.bus.kafka.{KafkaConsumer, KafkaTopicUtils, KafkaProducer}
import com.stratio.streaming.commons.constants.{BUS, CEPOperations, TopicNames}
import org.apache.curator.framework.api.{CuratorEvent, CuratorListener}
import org.apache.curator.framework.api.CuratorEventType._
import com.stratio.streaming.commons.constants.STREAMING._
import com.stratio.streaming.commons.exceptions.{StratioAPISecurityException, StratioEngineStatusException, StratioStreamingException}
import com.stratio.streaming.commons.streams.StratioStream

class StratioStreamingAPI
  extends IStratioStreamingAPI {
  import StratioStreamingAPI._

  def send(message: StratioStreamingMessage) = {
    checkStreamingStatus()
    checkSecurityConstraints(message)
    message.getOperation.toUpperCase match {
      case CEPOperations.CREATE | DROP | ALTER | LISTEN =>
        syncOperation.performSyncOperation(message)
      case INSERT | ADD_QUERY | LIST | SAVETO_CASSANDRA | SAVETO_DATACOLLECTOR =>
        asyncOperation.performAsyncOperation(message)
      case LIST  =>
        getStreamsList()
      case _ => throw new StratioStreamingException("Unknown operation")
    }
  }

  def initialize() = {
    checkEphemeralNode()
    startEphemeralNodeWatch()
    initializeTopic()
    this
  }

  def getStreamsList(): List[StratioStream] = {
    statusOperation.getStreamsList()
  }
}

object StratioStreamingAPI {
  val config = ConfigFactory.load()
  val streamingTopicName = TopicNames.STREAMING_REQUESTS_TOPIC
  val brokerServer = config.getString("broker.server")
  val brokerPort = config.getString("broker.port")
  val kafkaBroker = s"$brokerServer:$brokerPort"
  val zookeeperServer = config.getString("zookeeper.server")
  val zookeeperPort = config.getString("zookeeper.port")
  val zookeeperCluster = s"$zookeeperServer:$zookeeperPort"
  var streamingUpAndRunning = false

  lazy val kafkaProducer = new KafkaProducer(BUS.TOPICS, kafkaBroker)
  lazy val kafkaConsumer = new KafkaConsumer(BUS.LIST_STREAMS_TOPIC, zookeeperCluster)

  val retryPolicy = new ExponentialBackoffRetry(1000, 3)
  lazy val zookeeperClient = CuratorFrameworkFactory.newClient(zookeeperCluster, retryPolicy)
  lazy val zookeeperConsumer = {
    zookeeperClient.start()
    ZookeeperConsumer(zookeeperClient)
  }

  lazy val syncOperation = new StreamingAPISyncOperation(kafkaProducer, zookeeperConsumer)
  lazy val asyncOperation = new StreamingAPIAsyncOperation(kafkaProducer)
  lazy val statusOperation = new StreamingAPIListOperation(kafkaProducer, zookeeperConsumer)

  def checkEphemeralNode() {
    val ephemeralNodePath = ZK_EPHEMERAL_NODE_PATH
    if (!zookeeperConsumer.zNodeExists(ephemeralNodePath))
      throw new StratioEngineStatusException("Stratio streaming is down")
    else
      streamingUpAndRunning = true
  }

  def startEphemeralNodeWatch() {
    zookeeperClient.checkExists().watched().forPath(ZK_EPHEMERAL_NODE_PATH)
    addListener()
  }

  def initializeTopic() {
    KafkaTopicUtils.createTopicIfNotExists(zookeeperCluster, streamingTopicName)
  }

  def checkSecurityConstraints(message: StratioStreamingMessage) {
     if (message.getStreamName.startsWith("stratio_"))
       throw new StratioAPISecurityException("StratioStreamingAPI - the stream is not user defined")
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
