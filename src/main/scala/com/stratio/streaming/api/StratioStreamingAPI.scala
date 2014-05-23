/*
 * Copyright 2014 Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stratio.streaming.api

import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import com.stratio.streaming.commons.messages.{ColumnNameTypeValue, StratioStreamingMessage}
import com.stratio.streaming.kafka.KafkaConsumer
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
import com.stratio.streaming.messaging._
import com.stratio.streaming.messaging.MessageBuilder._
import scala.collection.JavaConversions._
import com.stratio.streaming.messaging.InsertMessageBuilder
import com.stratio.streaming.messaging.MessageBuilderWithColumns
import com.stratio.streaming.kafka.KafkaProducer
import com.stratio.streaming.dto.StratioQueryStream
import com.stratio.streaming.messaging.QueryMessageBuilder
import com.stratio.streaming.zookeeper.ZookeeperConsumer
import com.stratio.streaming.commons.kafka.service.{TopicService, KafkaTopicService}

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

  def addQuery(streamName: String, query: String): String = {
    checkStreamingStatus()
    val operation = ADD_QUERY.toLowerCase
    val addQueryStreamMessage = QueryMessageBuilder(sessionId).build(streamName, query, operation)
    checkSecurityConstraints(addQueryStreamMessage)
    syncOperation.performSyncOperation(addQueryStreamMessage)
    getQueryId(streamName, query)
  }

  def getQueryId(streamName: String, query: String): String = {
    val queries = queriesFromStream(streamName)
    val addedQuery = queries.find(theQuery => theQuery.query.equals(query))
    addedQuery match {
      case Some(q) => q.queryId
      case _ => ""
    }
  }

  def removeQuery(streamName: String, queryId: String) = {
    checkStreamingStatus()
    val operation = REMOVE_QUERY.toLowerCase
    val removeQueryMessage = QueryMessageBuilder(sessionId).build(streamName, queryId, operation)
    checkSecurityConstraints(removeQueryMessage)
    syncOperation.performSyncOperation(removeQueryMessage)
  }

  def dropStream(streamName: String) = {
    checkStreamingStatus()
    val operation = DROP.toLowerCase
    val dropStreamMessage = StreamMessageBuilder(sessionId).build(streamName, operation)
    checkSecurityConstraints(dropStreamMessage)
    syncOperation.performSyncOperation(dropStreamMessage)
  }

  def listenStream(streamName: String) = {
    checkStreamingStatus()
    val operation = LISTEN.toLowerCase
    val listenStreamMessage = StreamMessageBuilder(sessionId).build(streamName, operation)
    checkSecurityConstraints(listenStreamMessage)
    syncOperation.performSyncOperation(listenStreamMessage)
    val kafkaConsumer = new KafkaConsumer(streamName, zookeeperCluster)
    streamingListeners.put(streamName, kafkaConsumer)
    kafkaConsumer.stream
  }

  def stopListenStream(streamName: String) = {
    checkStreamingStatus()
    val operation = STOP_LISTEN.toLowerCase
    val stopListenStreamMessage = StreamMessageBuilder(sessionId).build(streamName, operation)
    checkSecurityConstraints(stopListenStreamMessage)
    shutdownKafkaConsumerAndRemoveStreamingListener(streamName)
    syncOperation.performSyncOperation(stopListenStreamMessage)
  }


  private def shutdownKafkaConsumerAndRemoveStreamingListener(streamName: String) {
    val kafkaConsumer = streamingListeners.get(streamName)
    kafkaConsumer match {
      case Some(consumer) => consumer.close()
      case _ => //
    }
    streamingListeners.remove(streamName)
  }

  def queriesFromStream(stream: String): List[StratioQueryStream] = {
    val stratioStreams = listStreams().toList
    val stratioStream = stratioStreams.find(element => element.getStreamName.equals(stream))
    stratioStream match {
      case None => throw new StratioEngineOperationException("StratioEngine error: STREAM DOES NOT EXIST")
      case Some(element) => element.getQueries.map(query => new StratioQueryStream(query.getQuery, query.getQueryId))
    }
  }

  def columnsFromStream(stream: String): List[ColumnNameTypeValue] = {
    val stratioStreams = listStreams().toList
    val stratioStream = stratioStreams.find(element => element.getStreamName.equals(stream))
    stratioStream match {
      case None => throw new StratioEngineOperationException("StratioEngine error: STREAM DOES NOT EXIST")
      case Some(element) => element.getColumns
    }
  }

  def listStreams(): List[StratioStream] = {
    checkStreamingStatus()
    val operation = LIST.toLowerCase
    val listStreamMessage = builder.withOperation(operation)
      .withSessionId(sessionId)
      .build()
    statusOperation.getListStreams(listStreamMessage)
  }

  def saveToCassandra(streamName: String) = {
    checkStreamingStatus()
    val operation = SAVETO_CASSANDRA.toLowerCase
    val saveToCassandraMessage = StreamMessageBuilder(sessionId).build(streamName, operation)
    syncOperation.performSyncOperation(saveToCassandraMessage)
  }
  
  def indexStream(streamName: String) = {
    checkStreamingStatus()
    val operation = INDEX.toLowerCase
    val indexStreamMessage = StreamMessageBuilder(sessionId).build(streamName, operation)
    syncOperation.performSyncOperation(indexStreamMessage)
  }

  def stopIndexStream(streamName: String) = {
    //TODO
    /*checkStreamingStatus()
    val operation = INDEX.toLowerCase
    val indexStreamMessage = StreamMessageBuilder(sessionId).build(streamName, operation)
    syncOperation.performSyncOperation(indexStreamMessage)*/
  }
  
  

  def initialize() = {
    brokerServer = config.getString("kafka.server")
    brokerPort = config.getInt("kafka.port")
    zookeeperServer = config.getString("zookeeper.server")
    zookeeperPort = config.getInt("zookeeper.port")
    checkEphemeralNode()
    startEphemeralNodeWatch()
    initializeTopic()
    this
  }

  def initializeWithServerConfig(kafkaServer: String,
                 kafkaPort: Int,
                 theZookeeperServer: String,
                 theZookeeperPort: Int) = {
    brokerServer = kafkaServer
    brokerPort = kafkaPort
    zookeeperServer = theZookeeperServer
    zookeeperPort = theZookeeperPort
    checkEphemeralNode()
    startEphemeralNodeWatch()
    initializeTopic()
    this
  }
}

object StratioStreamingAPI
 extends StratioStreamingAPIConfig {
  val streamingTopicName = TOPICS
  val sessionId = "" + System.currentTimeMillis()
  var brokerServer = ""
  var brokerPort = 0
  lazy val kafkaBroker = s"$brokerServer:$brokerPort"
  var zookeeperServer = ""
  var zookeeperPort = 0
  lazy val zookeeperCluster = s"$zookeeperServer:$zookeeperPort"
  var streamingUpAndRunning = false
  val streamingListeners = scala.collection.mutable.Map[String, KafkaConsumer]()
  lazy val kafkaProducer = new KafkaProducer(TOPICS, kafkaBroker)
  val retryPolicy = new ExponentialBackoffRetry(1000, 3)
  lazy val zookeeperClient = CuratorFrameworkFactory.newClient(zookeeperCluster, retryPolicy)
  var topicService: TopicService = _
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
    topicService = new KafkaTopicService(zookeeperCluster, brokerServer, brokerPort, 10000, 10000)
    topicService.createTopicIfNotExist(streamingTopicName, 1, 1)
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
