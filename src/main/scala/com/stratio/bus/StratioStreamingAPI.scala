package com.stratio.bus

import _root_.kafka.consumer.KafkaStream
import com.typesafe.config.ConfigFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import com.stratio.streaming.commons.messages.{ColumnNameTypeValue, StratioStreamingMessage}
import com.stratio.bus.kafka.{KafkaConsumer, KafkaTopicUtils}
import com.stratio.streaming.commons.constants.BUS._
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS.DEFINITION._
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS.ACTION._
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS.MANIPULATION._
import org.apache.curator.framework.api.{CuratorEvent, CuratorListener}
import org.apache.curator.framework.api.CuratorEventType._
import com.stratio.streaming.commons.constants.STREAMING._
import com.stratio.streaming.commons.exceptions.{StratioEngineOperationException, StratioAPISecurityException, StratioEngineStatusException}
import com.stratio.streaming.commons.streams.StratioStream
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS.DEFINITION
import java.util.List
import com.stratio.bus.messaging._
import com.stratio.bus.messaging.MessageBuilder._
import scala.collection.JavaConversions._
import com.stratio.bus.messaging.InsertMessageBuilder
import com.stratio.bus.messaging.MessageBuilderWithColumns
import com.stratio.bus.kafka.KafkaProducer
import com.stratio.bus.dto.StratioQueryStream
import com.stratio.bus.messaging.AddQueryMessageBuilder
import com.stratio.bus.zookeeper.ZookeeperConsumer

class StratioStreamingAPI
  extends IStratioStreamingAPI {
  import StratioStreamingAPI._

  def createStream(streamName: String, columns: List[ColumnNameType]) = {
    checkStreamingStatus()
    val operation = DEFINITION.CREATE.toLowerCase
    val creationStreamMessage = MessageBuilderWithColumns(sessionId, operation).build(streamName, columns)
    checkSecurityConstraints(creationStreamMessage)
    syncOperation.performSyncOperation(creationStreamMessage)
  }

  def alterStream(streamName: String, columns: List[ColumnNameType]) = {
    checkStreamingStatus()
    val operation = ALTER.toLowerCase
    val alterStreamMessage = MessageBuilderWithColumns(sessionId, operation).build(streamName, columns)
    checkSecurityConstraints(alterStreamMessage)
    syncOperation.performSyncOperation(alterStreamMessage)
  }

  def insertData(streamName: String, data: List[ColumnNameValue]) = {
    checkStreamingStatus()
    val insertStreamMessage = InsertMessageBuilder(sessionId).build(streamName, data)
    checkSecurityConstraints(insertStreamMessage)
    asyncOperation.performAsyncOperation(insertStreamMessage)
  }

  def addQuery(streamName: String, query: String) = {
    checkStreamingStatus()
    val addQueryStreamMessage = AddQueryMessageBuilder(sessionId).build(streamName, query)
    checkSecurityConstraints(addQueryStreamMessage)
    syncOperation.performSyncOperation(addQueryStreamMessage)
  }

  def removeQuery(streamName: String, queryId: String) = {
    //TODO IGUAL QUE ADDQUERY!!!!!!!!!!
    checkStreamingStatus()
    val operation = REMOVE_QUERY.toLowerCase
    val removeQueryMessage = builder.withOperation(operation)
        .withStreamName(streamName)
        .withSessionId(sessionId)
        .withRequest(queryId)
        .build()
    checkSecurityConstraints(removeQueryMessage)
    syncOperation.performSyncOperation(removeQueryMessage)
  }


  //TODO REFACTOR BUILDERS!!!!!!!!!!!!
  def dropStream(streamName: String) = {
    checkStreamingStatus()
    val operation = DROP.toLowerCase
    val dropStreamMessage = builder.withOperation(operation)
      .withStreamName(streamName)
      .withSessionId(sessionId)
      .build()
    checkSecurityConstraints(dropStreamMessage)
    syncOperation.performSyncOperation(dropStreamMessage)
  }

  def listenStream[T](streamName: String) = {
    checkStreamingStatus()
    val operation = LISTEN.toLowerCase
    val listenStreamMessage = builder.withOperation(operation)
      .withStreamName(streamName)
      .withSessionId(sessionId)
      .build()
    checkSecurityConstraints(listenStreamMessage)
    syncOperation.performSyncOperation(listenStreamMessage)
    val kafkaConsumer = new KafkaConsumer[T](streamName, zookeeperCluster)
    kafkaConsumer.stream
  }

  def stopListenStream(streamName: String) = {
    checkStreamingStatus()
    val operation = STOP_LISTEN.toLowerCase
    val listenStreamMessage = builder.withOperation(operation)
      .withStreamName(streamName)
      .withSessionId(sessionId)
      .build()
    checkSecurityConstraints(listenStreamMessage)
    syncOperation.performSyncOperation(listenStreamMessage)
  }

  def queriesFromStream(stream: String): List[StratioQueryStream] = {
    checkStreamingStatus()
    val stratioStreams = listStreams().toList
    val stratioStream = stratioStreams.find(element => element.getStreamName.equals(stream))
    stratioStream match {
      case None => throw new StratioEngineOperationException("StratioEngine error: STREAM DOES NOT EXIST")
      case Some(element) => element.getQueries.map(query => new StratioQueryStream(query.getQuery, query.getQueryId))
    }
  }

  def columnsFromStream(stream: String): List[ColumnNameTypeValue] = {
    checkStreamingStatus()
    val stratioStreams = listStreams().toList
    val stratioStream = stratioStreams.find(element => element.getStreamName.equals(stream))
    stratioStream match {
      case None => throw new StratioEngineOperationException("StratioEngine error: STREAM DOES NOT EXIST")
      case Some(element) => element.getColumns
    }
  }

  def listStreams(): List[StratioStream] = {
    val operation = LIST.toLowerCase
    val listStreamMessage = builder.withOperation(operation)
      .withSessionId(sessionId)
      .build()
    statusOperation.getListStreams(listStreamMessage)
  }

  def initialize() = {
    checkEphemeralNode()
    startEphemeralNodeWatch()
    initializeTopic()
    this
  }
}

object StratioStreamingAPI {
  val config = ConfigFactory.load()
  val streamingTopicName = TOPICS
  val sessionId = "" + System.currentTimeMillis()
  val brokerServer = config.getString("broker.server")
  val brokerPort = config.getString("broker.port")
  val kafkaBroker = s"$brokerServer:$brokerPort"
  val zookeeperServer = config.getString("zookeeper.server")
  val zookeeperPort = config.getString("zookeeper.port")
  val zookeeperCluster = s"$zookeeperServer:$zookeeperPort"
  var streamingUpAndRunning = false

  lazy val kafkaProducer = new KafkaProducer(TOPICS, kafkaBroker)
  //lazy val kafkaConsumer = new KafkaConsumer(LIST_STREAMS_TOPIC, zookeeperCluster)
 

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
