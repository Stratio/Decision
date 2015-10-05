/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.decision.integration.engine.api

import com.stratio.decision.api.messaging.{ColumnNameType, ColumnNameValue}
import com.stratio.decision.api.{StratioStreamingAPI, StreamingAPIAsyncOperation, StreamingAPIListOperation, StreamingAPISyncOperation}
import com.stratio.decision.commons.constants.ColumnType
import com.stratio.decision.commons.exceptions.{StratioEngineOperationException, StratioEngineStatusException}
import com.stratio.decision.commons.messages.{StratioStreamingMessage, StreamQuery}
import com.stratio.decision.commons.streams.StratioStream
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class StratioStreamingAPIUnitTests
  extends WordSpec
  with ShouldMatchers
  with MockitoSugar {

  trait DummyStratioStreamingAPI {
    val api = new StratioStreamingAPI()
    api.streamingUp = false
    api.streamingRunning = false

    val streamingAPIListOperationMock = mock[StreamingAPIListOperation]
    val streamingAPISyncOperationMock = mock[StreamingAPISyncOperation]
    val streamingAPIAsyncOperationMock = mock[StreamingAPIAsyncOperation]
  }

  "The Stratio Streaming API" when {
    "set the server config" should {
      "modifies the server config values" in new DummyStratioStreamingAPI {
        api.kafkaCluster = ""
        api.zookeeperServer = ""

        val newKafkaCluster = "kafkaQuorumTest"
        val newZookeeperServer = "zookeeperQuorumTest"

        val result = api.withServerConfig(newKafkaCluster, newZookeeperServer)

        api.kafkaCluster should be(newKafkaCluster)
        api.zookeeperServer should be(newZookeeperServer)
      }
    }

    "set the server config (with ports)" should {
      "modify the server config values" in new DummyStratioStreamingAPI {
        api.kafkaCluster = ""
        api.zookeeperServer = ""

        val newKafkaCluster = "kafkaQuorumTest:0"
        val newKafkaHost = "kafkaQuorumTest"
        val newKafkaPort = 0
        val newZookeeperServer = "zookeeperQuorumTest:0"
        val newZookeeperHost = "zookeeperQuorumTest"
        val newZookeeperPort = 0

        val result =
          api.withServerConfig(newKafkaHost, newKafkaPort, newZookeeperHost, newZookeeperPort)

        api.kafkaCluster should be(newKafkaCluster)
        api.zookeeperServer should be(newZookeeperServer)
      }
    }

    "list streams" should {
      "return a new StratioQueryStream" in new DummyStratioStreamingAPI {

        api.streamingUp = true
        api.streamingRunning = true
        val stratioStreamMock = mock[StratioStream]

        api.statusOperation = streamingAPIListOperationMock

        when(
          streamingAPIListOperationMock.getListStreams(any[StratioStreamingMessage])
        ).thenReturn(List(stratioStreamMock))

        api.listStreams().toList should be(List(stratioStreamMock))
      }

      "throw an exception if streaming is down" in new DummyStratioStreamingAPI {
        api.streamingUp = false

        val thrown = intercept[StratioEngineStatusException] {
          api.listStreams()
        }

        thrown.getMessage should be("Stratio streaming is down")
      }

      "throw an exception if streaming is not running" in new DummyStratioStreamingAPI {
        api.streamingUp = true
        api.streamingRunning = false

        val thrown = intercept[StratioEngineStatusException] {
          api.listStreams()
        }

        thrown.getMessage should be("Stratio streaming not yet initialized")
      }
    }

    "query a stream" should {

      val streamsList =
        """{"count":1,"timestamp":1402494388420,"streams":[{"streamName":"unitTestsStream",
          |"columns":[{"column":"column1","type":"STRING"}],"queries":[],"activeActions":[],
          |"userDefined":true}]}""".stripMargin

      "return a new StratioQueryStream" in new DummyStratioStreamingAPI {

        api.streamingUp = true
        api.streamingRunning = true
        val stratioStreamMock = mock[StratioStream]
        api.statusOperation = streamingAPIListOperationMock

        when(stratioStreamMock.getStreamName).thenReturn(streamsList)
        val streamQueryMock = mock[StreamQuery]
        when(streamQueryMock.getQuery).thenReturn("query")
        when(streamQueryMock.getQueryId).thenReturn("queryId")
        when(stratioStreamMock.getQueries).thenReturn(List(streamQueryMock))
        when(
          streamingAPIListOperationMock.getListStreams(any[StratioStreamingMessage])
        ).thenReturn(List(stratioStreamMock))

        val expected = new StratioQueryStream(streamQueryMock.getQuery, streamQueryMock.getQueryId)
        api.queriesFromStream(streamsList).toList should be(List(expected))
      }

      "throw an exception if there is not any coincidence" in new DummyStratioStreamingAPI {
        api.streamingUp = true
        api.streamingRunning = true
        val stratioStreamMock = mock[StratioStream]

        api.statusOperation = streamingAPIListOperationMock

        when(
          streamingAPIListOperationMock.getListStreams(any[StratioStreamingMessage])
        ).thenReturn(List(stratioStreamMock))
        when(stratioStreamMock.getStreamName).thenReturn("test")

        val thrown = intercept[StratioEngineOperationException] {
          api.queriesFromStream(streamsList)
        }

        thrown.getMessage should be("StratioEngine error: STREAM DOES NOT EXIST")
      }

      "throw an exception if there is not any stratio stream" in new DummyStratioStreamingAPI {
        api.streamingUp = true
        api.streamingRunning = true

        api.statusOperation = streamingAPIListOperationMock

        when(
          streamingAPIListOperationMock.getListStreams(any[StratioStreamingMessage])
        ).thenReturn(List())

        val thrown = intercept[StratioEngineOperationException] {
          api.queriesFromStream(streamsList)
        }

        thrown.getMessage should be("StratioEngine error: STREAM DOES NOT EXIST")
      }

      "throw an exception if streaming is down" in new DummyStratioStreamingAPI {
        api.streamingUp = false

        val thrown = intercept[StratioEngineStatusException] {
          api.queriesFromStream(streamsList)
        }

        thrown.getMessage should be("Stratio streaming is down")
      }

      "throw an exception if streaming is not running" in new DummyStratioStreamingAPI {
        api.streamingUp = true
        api.streamingRunning = false

        val thrown = intercept[StratioEngineStatusException] {
          api.queriesFromStream(streamsList)
        }

        thrown.getMessage should be("Stratio streaming not yet initialized")
      }
    }

    "index a stream" should {

      val streamName = "unitTestsStream"

      "index a stratio stream" in new DummyStratioStreamingAPI {

        api.streamingUp = true
        api.streamingRunning = true
        api.syncOperation = streamingAPISyncOperationMock

        doNothing().when(streamingAPISyncOperationMock).performSyncOperation(any[StratioStreamingMessage])

        api.indexStream(streamName)
      }

      "throw an exception if streaming is down" in new DummyStratioStreamingAPI {
        api.streamingUp = false

        val thrown = intercept[StratioEngineStatusException] {
          api.indexStream(streamName)
        }

        thrown.getMessage should be("Stratio streaming is down")
      }

      "throw an exception if streaming is not running" in new DummyStratioStreamingAPI {
        api.streamingUp = true
        api.streamingRunning = false

        val thrown = intercept[StratioEngineStatusException] {
          api.indexStream(streamName)
        }

        thrown.getMessage should be("Stratio streaming not yet initialized")
      }
    }

    "stop index stream" should {

      val streamName = "unitTestsStream"

      "stop an index stream" in new DummyStratioStreamingAPI {

        api.streamingUp = true
        api.streamingRunning = true
        api.syncOperation = streamingAPISyncOperationMock

        doNothing().when(streamingAPISyncOperationMock).performSyncOperation(any[StratioStreamingMessage])

        api.stopIndexStream(streamName)
      }

      "throw an exception if streaming is down" in new DummyStratioStreamingAPI {
        api.streamingUp = false

        val thrown = intercept[StratioEngineStatusException] {
          api.stopIndexStream(streamName)
        }

        thrown.getMessage should be("Stratio streaming is down")
      }

      "throw an exception if streaming is not running" in new DummyStratioStreamingAPI {
        api.streamingUp = true
        api.streamingRunning = false

        val thrown = intercept[StratioEngineStatusException] {
          api.stopIndexStream(streamName)
        }

        thrown.getMessage should be("Stratio streaming not yet initialized")
      }
    }

    "create stream" should {

      val streamName = "unitTestsStream"
      val columnNameType = new ColumnNameType("columnName", ColumnType.INTEGER)
      val columns = List(columnNameType)

      "create a stream" in new DummyStratioStreamingAPI {

        api.streamingUp = true
        api.streamingRunning = true
        api.syncOperation = streamingAPISyncOperationMock

        doNothing().when(streamingAPISyncOperationMock).performSyncOperation(any[StratioStreamingMessage])

        api.createStream(streamName, columns)
      }

      "throw an exception if streaming is down" in new DummyStratioStreamingAPI {
        api.streamingUp = false

        val thrown = intercept[StratioEngineStatusException] {
          api.createStream(streamName, columns)
        }

        thrown.getMessage should be("Stratio streaming is down")
      }

      "throw an exception if streaming is not running" in new DummyStratioStreamingAPI {
        api.streamingUp = true
        api.streamingRunning = false

        val thrown = intercept[StratioEngineStatusException] {
          api.createStream(streamName, columns)
        }

        thrown.getMessage should be("Stratio streaming not yet initialized")
      }
    }

    "alter stream" should {

      val streamName = "unitTestsStream"
      val columnNameType = new ColumnNameType("columnName", ColumnType.INTEGER)
      val columns = List(columnNameType)

      "alter a stream" in new DummyStratioStreamingAPI {

        api.streamingUp = true
        api.streamingRunning = true
        api.syncOperation = streamingAPISyncOperationMock

        doNothing().when(streamingAPISyncOperationMock).performSyncOperation(any[StratioStreamingMessage])

        api.alterStream(streamName, columns)
      }

      "throw an exception if streaming is down" in new DummyStratioStreamingAPI {
        api.streamingUp = false

        val thrown = intercept[StratioEngineStatusException] {
          api.alterStream(streamName, columns)
        }

        thrown.getMessage should be("Stratio streaming is down")
      }

      "throw an exception if streaming is not running" in new DummyStratioStreamingAPI {
        api.streamingUp = true
        api.streamingRunning = false

        val thrown = intercept[StratioEngineStatusException] {
          api.alterStream(streamName, columns)
        }

        thrown.getMessage should be("Stratio streaming not yet initialized")
      }
    }

    "insert data" should {

      val streamName = "unitTestsStream"
      val columnNameValue = new ColumnNameValue("columnName", ColumnType.INTEGER)
      val data = List(columnNameValue)

      "insert data" in new DummyStratioStreamingAPI {

        api.streamingUp = true
        api.streamingRunning = true
        api.syncOperation = streamingAPISyncOperationMock

        doNothing().when(streamingAPIAsyncOperationMock).performAsyncOperation(any[StratioStreamingMessage])

        api.insertData(streamName, data)
      }

      "throw an exception if streaming is down" in new DummyStratioStreamingAPI {
        api.streamingUp = false

        val thrown = intercept[StratioEngineStatusException] {
          api.insertData(streamName, data)
        }

        thrown.getMessage should be("Stratio streaming is down")
      }

      "throw an exception if streaming is not running" in new DummyStratioStreamingAPI {
        api.streamingUp = true
        api.streamingRunning = false

        val thrown = intercept[StratioEngineStatusException] {
          api.insertData(streamName, data)
        }

        thrown.getMessage should be("Stratio streaming not yet initialized")
      }
    }

    "get query id" should {

      val streamName = "unitTestsStream"
      val query = "from unitTestsStream select column1 insert into alarms for current-events"

      "return the query id" in new DummyStratioStreamingAPI {
        api.streamingUp = true
        api.streamingRunning = true
        val stratioStreamMock = mock[StratioStream]
        api.statusOperation = streamingAPIListOperationMock

        when(stratioStreamMock.getStreamName).thenReturn(streamName)
        val streamQueryMock = mock[StreamQuery]
        val queryId = "queryId"
        when(streamQueryMock.getQuery).thenReturn(query)
        when(streamQueryMock.getQueryId).thenReturn(queryId)
        when(stratioStreamMock.getQueries).thenReturn(List(streamQueryMock))
        when(
          streamingAPIListOperationMock.getListStreams(any[StratioStreamingMessage])
        ).thenReturn(List(stratioStreamMock))

        api.getQueryId(streamName, query) should be(queryId)
      }

      "return an empty String if the query is not found" in new DummyStratioStreamingAPI {
        api.streamingUp = true
        api.streamingRunning = true
        val stratioStreamMock = mock[StratioStream]
        api.statusOperation = streamingAPIListOperationMock

        when(stratioStreamMock.getStreamName).thenReturn(streamName)
        val streamQueryMock = mock[StreamQuery]
        when(streamQueryMock.getQuery).thenReturn("query")
        when(streamQueryMock.getQueryId).thenReturn("queryId")
        when(stratioStreamMock.getQueries).thenReturn(List(streamQueryMock))
        when(
          streamingAPIListOperationMock.getListStreams(any[StratioStreamingMessage])
        ).thenReturn(List(stratioStreamMock))

        api.getQueryId(streamName, query) should be("")
      }

      "throw an exception if there is not any coincidence" in new DummyStratioStreamingAPI {
        api.streamingUp = true
        api.streamingRunning = true
        val stratioStreamMock = mock[StratioStream]

        api.statusOperation = streamingAPIListOperationMock

        when(
          streamingAPIListOperationMock.getListStreams(any[StratioStreamingMessage])
        ).thenReturn(List(stratioStreamMock))
        when(stratioStreamMock.getStreamName).thenReturn("test")

        val thrown = intercept[StratioEngineOperationException] {
          api.getQueryId(streamName, query)
        }

        thrown.getMessage should be("StratioEngine error: STREAM DOES NOT EXIST")
      }

      "throw an exception if there is not any stratio stream" in new DummyStratioStreamingAPI {
        api.streamingUp = true
        api.streamingRunning = true

        api.statusOperation = streamingAPIListOperationMock

        when(
          streamingAPIListOperationMock.getListStreams(any[StratioStreamingMessage])
        ).thenReturn(List())

        val thrown = intercept[StratioEngineOperationException] {
          api.getQueryId(streamName, query)
        }

        thrown.getMessage should be("StratioEngine error: STREAM DOES NOT EXIST")
      }

      "throw an exception if streaming is down" in new DummyStratioStreamingAPI {
        api.streamingUp = false

        val thrown = intercept[StratioEngineStatusException] {
          api.getQueryId(streamName, query)
        }

        thrown.getMessage should be("Stratio streaming is down")
      }

      "throw an exception if streaming is not running" in new DummyStratioStreamingAPI {
        api.streamingUp = true
        api.streamingRunning = false

        val thrown = intercept[StratioEngineStatusException] {
          api.getQueryId(streamName, query)
        }

        thrown.getMessage should be("Stratio streaming not yet initialized")
      }
    }

    "add query" should {

      val streamName = "unitTestsStream"
      val query = "from unitTestsStream select column1 insert into alarms for current-events"

      "return the query id" in new DummyStratioStreamingAPI {
        api.streamingUp = true
        api.streamingRunning = true
        val stratioStreamMock = mock[StratioStream]
        api.statusOperation = streamingAPIListOperationMock
        api.syncOperation = streamingAPISyncOperationMock

        when(stratioStreamMock.getStreamName).thenReturn(streamName)
        val streamQueryMock = mock[StreamQuery]
        val queryId = "queryId"
        when(streamQueryMock.getQuery).thenReturn(query)
        when(streamQueryMock.getQueryId).thenReturn(queryId)
        when(stratioStreamMock.getQueries).thenReturn(List(streamQueryMock))
        when(
          streamingAPIListOperationMock.getListStreams(any[StratioStreamingMessage])
        ).thenReturn(List(stratioStreamMock))



        api.addQuery(streamName, query) should be(queryId)
      }

      "return an empty String if the query is not found" in new DummyStratioStreamingAPI {
        api.streamingUp = true
        api.streamingRunning = true
        val stratioStreamMock = mock[StratioStream]
        api.statusOperation = streamingAPIListOperationMock
        api.syncOperation = streamingAPISyncOperationMock

        when(stratioStreamMock.getStreamName).thenReturn(streamName)
        val streamQueryMock = mock[StreamQuery]
        when(streamQueryMock.getQuery).thenReturn("query")
        when(streamQueryMock.getQueryId).thenReturn("queryId")
        when(stratioStreamMock.getQueries).thenReturn(List(streamQueryMock))
        when(
          streamingAPIListOperationMock.getListStreams(any[StratioStreamingMessage])
        ).thenReturn(List(stratioStreamMock))

        doNothing().when(streamingAPISyncOperationMock).performSyncOperation(any[StratioStreamingMessage])

        api.addQuery(streamName, query) should be("")
      }

      "throw an exception if there is not any coincidence" in new DummyStratioStreamingAPI {
        api.streamingUp = true
        api.streamingRunning = true
        val stratioStreamMock = mock[StratioStream]
        api.statusOperation = streamingAPIListOperationMock
        api.syncOperation = streamingAPISyncOperationMock

        when(
          streamingAPIListOperationMock.getListStreams(any[StratioStreamingMessage])
        ).thenReturn(List(stratioStreamMock))
        when(stratioStreamMock.getStreamName).thenReturn("test")

        doNothing().when(streamingAPISyncOperationMock).performSyncOperation(any[StratioStreamingMessage])

        val thrown = intercept[StratioEngineOperationException] {
          api.addQuery(streamName, query)
        }

        thrown.getMessage should be("StratioEngine error: STREAM DOES NOT EXIST")
      }

      "throw an exception if there is not any stratio stream" in new DummyStratioStreamingAPI {
        api.streamingUp = true
        api.streamingRunning = true

        api.statusOperation = streamingAPIListOperationMock
        api.syncOperation = streamingAPISyncOperationMock

        when(
          streamingAPIListOperationMock.getListStreams(any[StratioStreamingMessage])
        ).thenReturn(List())

        doNothing().when(streamingAPISyncOperationMock).performSyncOperation(any[StratioStreamingMessage])

        val thrown = intercept[StratioEngineOperationException] {
          api.addQuery(streamName, query)
        }

        thrown.getMessage should be("StratioEngine error: STREAM DOES NOT EXIST")
      }

      "throw an exception if streaming is down" in new DummyStratioStreamingAPI {
        api.streamingUp = false

        val thrown = intercept[StratioEngineStatusException] {
          api.addQuery(streamName, query)
        }

        thrown.getMessage should be("Stratio streaming is down")
      }

      "throw an exception if streaming is not running" in new DummyStratioStreamingAPI {
        api.streamingUp = true
        api.streamingRunning = false

        val thrown = intercept[StratioEngineStatusException] {
          api.addQuery(streamName, query)
        }

        thrown.getMessage should be("Stratio streaming not yet initialized")
      }
    }

    "remove query" should {

      val streamName = "unitTestsStream"
      val queryId = "queryId"

      "remove a query" in new DummyStratioStreamingAPI {

        api.streamingUp = true
        api.streamingRunning = true
        api.syncOperation = streamingAPISyncOperationMock

        doNothing().when(streamingAPISyncOperationMock).performSyncOperation(any[StratioStreamingMessage])

        api.removeQuery(streamName, queryId)
      }

      "throw an exception if streaming is down" in new DummyStratioStreamingAPI {
        api.streamingUp = false

        val thrown = intercept[StratioEngineStatusException] {
          api.removeQuery(streamName, queryId)
        }

        thrown.getMessage should be("Stratio streaming is down")
      }

      "throw an exception if streaming is not running" in new DummyStratioStreamingAPI {
        api.streamingUp = true
        api.streamingRunning = false

        val thrown = intercept[StratioEngineStatusException] {
          api.removeQuery(streamName, queryId)
        }

        thrown.getMessage should be("Stratio streaming not yet initialized")
      }
    }

    "save to Cassandra" should {

      val streamName = "unitTestsStream"

      "save to Cassandra" in new DummyStratioStreamingAPI {

        api.streamingUp = true
        api.streamingRunning = true
        api.syncOperation = streamingAPISyncOperationMock

        doNothing().when(streamingAPISyncOperationMock).performSyncOperation(any[StratioStreamingMessage])

        api.saveToCassandra(streamName)
      }

      "throw an exception if streaming is down" in new DummyStratioStreamingAPI {
        api.streamingUp = false

        val thrown = intercept[StratioEngineStatusException] {
          api.saveToCassandra(streamName)
        }

        thrown.getMessage should be("Stratio streaming is down")
      }

      "throw an exception if streaming is not running" in new DummyStratioStreamingAPI {
        api.streamingUp = true
        api.streamingRunning = false

        val thrown = intercept[StratioEngineStatusException] {
          api.saveToCassandra(streamName)
        }

        thrown.getMessage should be("Stratio streaming not yet initialized")
      }
    }

    "stop save to Cassandra" should {

      val streamName = "unitTestsStream"

      "stop save to Cassandra" in new DummyStratioStreamingAPI {

        api.streamingUp = true
        api.streamingRunning = true
        api.syncOperation = streamingAPISyncOperationMock

        doNothing().when(streamingAPISyncOperationMock).performSyncOperation(any[StratioStreamingMessage])

        api.stopSaveToCassandra(streamName)
      }

      "throw an exception if streaming is down" in new DummyStratioStreamingAPI {
        api.streamingUp = false

        val thrown = intercept[StratioEngineStatusException] {
          api.stopSaveToCassandra(streamName)
        }

        thrown.getMessage should be("Stratio streaming is down")
      }

      "throw an exception if streaming is not running" in new DummyStratioStreamingAPI {
        api.streamingUp = true
        api.streamingRunning = false

        val thrown = intercept[StratioEngineStatusException] {
          api.stopSaveToCassandra(streamName)
        }

        thrown.getMessage should be("Stratio streaming not yet initialized")
      }
    }

    "save to Mongo" should {

      val streamName = "unitTestsStream"

      "save to Mongo" in new DummyStratioStreamingAPI {

        api.streamingUp = true
        api.streamingRunning = true
        api.syncOperation = streamingAPISyncOperationMock

        doNothing().when(streamingAPISyncOperationMock).performSyncOperation(any[StratioStreamingMessage])

        api.saveToMongo(streamName)
      }

      "throw an exception if streaming is down" in new DummyStratioStreamingAPI {
        api.streamingUp = false

        val thrown = intercept[StratioEngineStatusException] {
          api.saveToMongo(streamName)
        }

        thrown.getMessage should be("Stratio streaming is down")
      }

      "throw an exception if streaming is not running" in new DummyStratioStreamingAPI {
        api.streamingUp = true
        api.streamingRunning = false

        val thrown = intercept[StratioEngineStatusException] {
          api.saveToMongo(streamName)
        }

        thrown.getMessage should be("Stratio streaming not yet initialized")
      }
    }

    "stop save to Mongo" should {

      val streamName = "unitTestsStream"

      "stop save to Mongo" in new DummyStratioStreamingAPI {

        api.streamingUp = true
        api.streamingRunning = true
        api.syncOperation = streamingAPISyncOperationMock

        doNothing().when(streamingAPISyncOperationMock).performSyncOperation(any[StratioStreamingMessage])

        api.stopSaveToMongo(streamName)
      }

      "throw an exception if streaming is down" in new DummyStratioStreamingAPI {
        api.streamingUp = false

        val thrown = intercept[StratioEngineStatusException] {
          api.stopSaveToMongo(streamName)
        }

        thrown.getMessage should be("Stratio streaming is down")
      }

      "throw an exception if streaming is not running" in new DummyStratioStreamingAPI {
        api.streamingUp = true
        api.streamingRunning = false

        val thrown = intercept[StratioEngineStatusException] {
          api.stopSaveToMongo(streamName)
        }

        thrown.getMessage should be("Stratio streaming not yet initialized")
      }
    }

    "save to Solr" should {

      val streamName = "unitTestsStream"

      "save to Solr" in new DummyStratioStreamingAPI {

        api.streamingUp = true
        api.streamingRunning = true
        api.syncOperation = streamingAPISyncOperationMock

        doNothing().when(streamingAPISyncOperationMock).performSyncOperation(any[StratioStreamingMessage])

        api.saveToSolr(streamName)
      }

      "throw an exception if streaming is down" in new DummyStratioStreamingAPI {
        api.streamingUp = false

        val thrown = intercept[StratioEngineStatusException] {
          api.saveToSolr(streamName)
        }

        thrown.getMessage should be("Stratio streaming is down")
      }

      "throw an exception if streaming is not running" in new DummyStratioStreamingAPI {
        api.streamingUp = true
        api.streamingRunning = false

        val thrown = intercept[StratioEngineStatusException] {
          api.saveToSolr(streamName)
        }

        thrown.getMessage should be("Stratio streaming not yet initialized")
      }
    }

    "stop save to Solr" should {

      val streamName = "unitTestsStream"

      "stop save to Solr" in new DummyStratioStreamingAPI {

        api.streamingUp = true
        api.streamingRunning = true
        api.syncOperation = streamingAPISyncOperationMock

        doNothing().when(streamingAPISyncOperationMock).performSyncOperation(any[StratioStreamingMessage])

        api.stopSaveToSolr(streamName)
      }

      "throw an exception if streaming is down" in new DummyStratioStreamingAPI {
        api.streamingUp = false

        val thrown = intercept[StratioEngineStatusException] {
          api.stopSaveToSolr(streamName)
        }

        thrown.getMessage should be("Stratio streaming is down")
      }

      "throw an exception if streaming is not running" in new DummyStratioStreamingAPI {
        api.streamingUp = true
        api.streamingRunning = false

        val thrown = intercept[StratioEngineStatusException] {
          api.stopSaveToSolr(streamName)
        }

        thrown.getMessage should be("Stratio streaming not yet initialized")
      }
    }

    "check if the system is initialized" should {
      "return true if the system is up and running" in new DummyStratioStreamingAPI {

        api.streamingUp = true
        api.streamingRunning = true

        api.isInit should be(true)
      }

      "return false if the system is down" in new DummyStratioStreamingAPI {

        api.streamingUp = false
        api.streamingRunning = true

        api.isInit should be(false)
      }

      "return false if the system is not running" in new DummyStratioStreamingAPI {

        api.streamingUp = true
        api.streamingRunning = false

        api.isInit should be(false)
      }
    }

    "check if the system is connected" should {
      "return true if the system is up and running" in new DummyStratioStreamingAPI {

        api.streamingUp = true
        api.streamingRunning = true

        api.isConnected should be(true)
      }

      "return false if the system is down" in new DummyStratioStreamingAPI {

        api.streamingUp = false
        api.streamingRunning = true

        api.isConnected should be(false)
      }

      "return false if the system is not running" in new DummyStratioStreamingAPI {

        api.streamingUp = true
        api.streamingRunning = false

        api.isConnected should be(false)
      }
    }

    "define acknowledge timeout" should {
      "define an acknowledge timeout" in new DummyStratioStreamingAPI {

        val newTimeout = 1

        api.ackTimeOut should be(8000)

        api.defineAcknowledgeTimeOut(newTimeout)

        api.ackTimeOut should be(newTimeout)
      }
    }
  }
}
