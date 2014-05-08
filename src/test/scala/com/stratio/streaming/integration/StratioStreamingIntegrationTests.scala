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

package com.stratio.streaming.integration

import org.scalatest._
import com.stratio.streaming.commons.exceptions.{StratioEngineOperationException, StratioStreamingException, StratioEngineStatusException, StratioAPISecurityException}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.framework.CuratorFrameworkFactory
import com.stratio.streaming.commons.constants.STREAMING._
import com.stratio.streaming.zookeeper.ZookeeperConsumer
import com.stratio.streaming.messaging.{ColumnNameValue, ColumnNameType}
import com.stratio.streaming.commons.constants.ColumnType
import scala.collection.JavaConversions._
import util.control.Breaks._
import java.util
import com.stratio.streaming.commons.messages.ColumnNameTypeValue
import com.stratio.streaming.api.StratioStreamingAPIFactory

class StratioStreamingIntegrationTests
  extends FunSpec
  with ShouldMatchers
  with BeforeAndAfterEach {

  val zookeeperCluster = "localhost:2181"
  val retryPolicy = new ExponentialBackoffRetry(1000, 3)
  val zookeeperClient = CuratorFrameworkFactory.newClient(zookeeperCluster, retryPolicy)
  zookeeperClient.start()
  val zookeeperConsumer = new ZookeeperConsumer(zookeeperClient)
  lazy val streamingAPI = StratioStreamingAPIFactory.create().initialize()
  val testStreamName = "unitTestsStream"
  val internalTestStreamName = "stratio_"


  override def beforeEach() {
    if (!zookeeperConsumer.zNodeExists(ZK_EPHEMERAL_NODE_PATH)) {
      zookeeperClient.create().forPath(ZK_EPHEMERAL_NODE_PATH)
      //Delay to get rid of flakiness
      Thread.sleep(1000)
    }
    cleanStratioStreamingEngine()
  }

  describe("The create operation") {
    it("should create a new stream when the stream does not exist") {
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
      val secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING)
      val thirdStreamColumn = new ColumnNameType("column3", ColumnType.BOOLEAN)
      val fourthStreamColumn = new ColumnNameType("column4", ColumnType.DOUBLE)
      val fifthStreamColumn = new ColumnNameType("column5", ColumnType.FLOAT)
      val sixthStreamColumn = new ColumnNameType("column6", ColumnType.LONG)
      val columnList = Seq(firstStreamColumn,
        secondStreamColumn,
        thirdStreamColumn,
        fourthStreamColumn,
        fifthStreamColumn,
        sixthStreamColumn)
      try {
        streamingAPI.createStream(testStreamName, columnList)
      } catch {
        case ssEx: StratioStreamingException => fail()
      }

      theNumberOfUserDefinedStreams should be(1)
      theNumberOfColumnsOfTheStream(testStreamName) should be(6)
    }

    it("should throw a StratioEngineOperationException when creating a stream that already exists") {
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
      val secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING)
      val columnList = Seq(firstStreamColumn, secondStreamColumn)
      intercept [StratioEngineOperationException] {
        streamingAPI.createStream(testStreamName, columnList)
        streamingAPI.createStream(testStreamName, columnList)
      }
    }

    it("should throw a StratioAPISecurityException when creating a stream with the stratio_ prefix") {
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
      val secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING)
      val columnList = Seq(firstStreamColumn, secondStreamColumn)
      intercept [StratioAPISecurityException] {
        streamingAPI.createStream(internalTestStreamName, columnList)
      }
    }

    it("should throw a StratioEngineStatusException when streaming engine is not running") {
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
      val secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING)
      val columnList = Seq(firstStreamColumn, secondStreamColumn)
      removeEphemeralNode()
      //Add some delay to wait for the event to be triggered
      Thread.sleep(1000)
      intercept [StratioEngineStatusException] {
        streamingAPI.createStream(testStreamName, columnList)
      }
    }
  }

  describe("The insert operation") {
    it("should throw a StratioEngineStatusException when streaming engine is not running") {
      val firstColumnValue = new ColumnNameValue("column1", new Integer(111111))
      val secondColumnValue = new ColumnNameValue("column2", "testString")
      val columnValues = Seq(firstColumnValue, secondColumnValue)
      removeEphemeralNode()
      //Add some delay to wait for the event to be triggered
      Thread.sleep(1000)
      intercept [StratioEngineStatusException] {
        streamingAPI.insertData(testStreamName, columnValues)
      }
    }

    it("should throw a StratioAPISecurityException when insert data into a stream with the stratio_ prefix") {
      val firstColumnValue = new ColumnNameValue("column1", new Integer(111111))
      val secondColumnValue = new ColumnNameValue("column2", "testString")
      val columnValues = Seq(firstColumnValue, secondColumnValue)
      intercept [StratioAPISecurityException] {
        streamingAPI.insertData(internalTestStreamName, columnValues)
      }
    }
  }

  describe("The alter operation") {
    it("should add new columns to an existing stream") {
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
      val secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING)
      val columnList = Seq(firstStreamColumn, secondStreamColumn)
      val thirdStreamColumn = new ColumnNameType("column3", ColumnType.INTEGER)
      val newColumnList = Seq(thirdStreamColumn)
      try {
        streamingAPI.createStream(testStreamName, columnList)
        streamingAPI.alterStream(testStreamName, newColumnList)
      } catch {
        case ssEx: StratioStreamingException => fail()
      }

      theNumberOfUserDefinedStreams should be(1)
      theNumberOfColumnsOfTheStream(testStreamName) should be(3)
    }

    it("should throw a StratioEngineOperationException when adding a column that already exists") {
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
      val secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING)
      val columnList = Seq(firstStreamColumn, secondStreamColumn)
      val thirdStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
      val newColumnList = Seq(thirdStreamColumn)
      streamingAPI.createStream(testStreamName, columnList)
      intercept [StratioEngineOperationException] {
        streamingAPI.alterStream(testStreamName, newColumnList)
      }
    }

    it("should throw a StratioEngineOperationException when adding a column to a non existing stream") {
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
      val secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING)
      val columnList = Seq(firstStreamColumn, secondStreamColumn)
      intercept [StratioEngineOperationException] {
        streamingAPI.alterStream(testStreamName, columnList)
      }
    }

    it("should throw a StratioAPISecurityException when adding a column to a non user-defined stream") {
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
      val secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING)
      val columnList = Seq(firstStreamColumn, secondStreamColumn)
      val thirdStreamColumn = new ColumnNameType("column3", ColumnType.INTEGER)
      val newColumnList = Seq(thirdStreamColumn)
      val internalStream = "stratio_stats_base"
      //Creates a stream to trigger the internal streams creation
      streamingAPI.createStream(testStreamName, columnList)
      intercept [StratioAPISecurityException] {
        streamingAPI.alterStream(internalStream, newColumnList)
      }
    }

    it("should throw a StratioEngineStatusException when streaming engine is not running") {
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
      val secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING)
      val columnList = Seq(firstStreamColumn, secondStreamColumn)
      val thirdStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
      val newColumnList = Seq(thirdStreamColumn)
      streamingAPI.createStream(testStreamName, columnList)
      removeEphemeralNode()
      Thread.sleep(1000)
      intercept [StratioEngineStatusException] {
        streamingAPI.alterStream(testStreamName, newColumnList)
      }
    }
  }

  describe("The add query operation") {
    it("should add new queries to an existing stream") {
      val alarmsStream = "alarms"
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
      val secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING)
      val thirdStreamColumn = new ColumnNameType("column3", ColumnType.BOOLEAN)
      val fourthStreamColumn = new ColumnNameType("column4", ColumnType.DOUBLE)
      val fifthStreamColumn = new ColumnNameType("column5", ColumnType.FLOAT)
      val sixthStreamColumn = new ColumnNameType("column6", ColumnType.LONG)
      val columnList = Seq(firstStreamColumn,
        secondStreamColumn,
        thirdStreamColumn,
        fourthStreamColumn,
        fifthStreamColumn,
        sixthStreamColumn)
      val theFirstQuery = s"from $testStreamName select column1, column2, column3, column4, column5, column6 insert into $alarmsStream for current-events"
      try {
        streamingAPI.createStream(alarmsStream, columnList)
        streamingAPI.createStream(testStreamName, columnList)
        streamingAPI.addQuery(testStreamName, theFirstQuery)
      } catch {
        case ssEx: StratioStreamingException => fail()
      }

      theNumberOfQueriesOfTheStream(testStreamName) should be(1)
    }

    it("should throw a StratioEngineOperationException when creating a wrong query") {
      val alarmsStream = "alarms"
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
      val secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING)
      val columnList = Seq(firstStreamColumn, secondStreamColumn)
      val theQuery = s"from $testStreamName select column1, column2, column3"
      streamingAPI.createStream(alarmsStream, columnList)
      streamingAPI.createStream(testStreamName, columnList)
      intercept [StratioEngineOperationException] {
          streamingAPI.addQuery(testStreamName, theQuery)
      }
    }

    it("should throw a StratioEngineStatusException when streaming engine is not running") {
      val alarmsStream = "alarms"
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
      val secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING)
      val columnList = Seq(firstStreamColumn, secondStreamColumn)
      val theQuery = s"from $testStreamName select column1, column2 insert into $alarmsStream for current-events"
      streamingAPI.createStream(alarmsStream, columnList)
      streamingAPI.createStream(testStreamName, columnList)
      removeEphemeralNode()
      Thread.sleep(2000)
      intercept [StratioEngineStatusException] {
        streamingAPI.addQuery(testStreamName, theQuery)
      }
    }

    it("should throw a StratioAPISecurityException when adding a query to a non user-defined stream") {
      val internalStream = "stratio_stats_base"
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
      val secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING)
      val columnList = Seq(firstStreamColumn, secondStreamColumn)
      val theQuery = s"from $testStreamName select column1, column2 insert into $internalStream for current-events"
      streamingAPI.createStream(testStreamName, columnList)
      intercept [StratioAPISecurityException] {
        streamingAPI.addQuery(internalStream, theQuery)
      }
    }
  }

  describe("The remove query operation") {
    it("should remove the queries from an existing stream") {
      val alarmsStream = "alarms"
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
      val secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING)
      val columnList = Seq(firstStreamColumn, secondStreamColumn)
      val theQuery = s"from $testStreamName select column1, column2 insert into $alarmsStream for current-events"
      try {
        streamingAPI.createStream(alarmsStream, columnList)
        streamingAPI.createStream(testStreamName, columnList)
        val queryId = streamingAPI.addQuery(testStreamName, theQuery)
        streamingAPI.removeQuery(testStreamName, queryId)
      } catch {
        case ssEx: StratioStreamingException => fail()
      }

      theNumberOfQueriesOfTheStream(testStreamName) should be(0)
    }

    it("should throw a StratioEngineOperationException when removing a non existing query") {
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
      val secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING)
      val columnList = Seq(firstStreamColumn, secondStreamColumn)
      val nonExistingQueryId = "1234"
      intercept [StratioEngineOperationException] {
        streamingAPI.createStream(testStreamName, columnList)
        streamingAPI.removeQuery(testStreamName, nonExistingQueryId)
      }
    }

    it("should throw a StratioEngineStatusException when streaming engine is not running") {
      val alarmsStream = "alarms"
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
      val secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING)
      val columnList = Seq(firstStreamColumn, secondStreamColumn)
      val theQuery = s"from $testStreamName select column1, column2 insert into $alarmsStream for current-events"
      streamingAPI.createStream(alarmsStream, columnList)
      streamingAPI.createStream(testStreamName, columnList)
      val queryId = streamingAPI.addQuery(testStreamName, theQuery)
      removeEphemeralNode()
      Thread.sleep(1000)
      intercept [StratioEngineStatusException] {
        streamingAPI.removeQuery(testStreamName, queryId)
      }
    }
  }

  describe("The drop operation") {
    it("should remove an existing stream") {
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
      val secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING)

      val columnList = Seq(firstStreamColumn, secondStreamColumn)
      try {
        streamingAPI.createStream(testStreamName, columnList)
        streamingAPI.dropStream(testStreamName)
      } catch {
        case ssEx: StratioStreamingException => fail()
      }
      theNumberOfUserDefinedStreams should be(0)
    }

    it("should throw a StratioEngineOperationException when removing a stream that does not exist") {
      val nonExistingStream = "nonExistingStream"
      intercept [StratioEngineOperationException] {
        streamingAPI.dropStream(nonExistingStream)
      }
    }

    it("should throw a StratioAPISecurityException when removing a stream with the stratio_ prefix") {
      intercept [StratioAPISecurityException] {
        streamingAPI.dropStream(internalTestStreamName)
      }
    }

    it("should throw a StratioEngineStatusException when streaming engine is not running") {
      removeEphemeralNode()
      //Add some delay to wait for the event to be triggered
      Thread.sleep(1000)
      intercept [StratioEngineStatusException] {
        streamingAPI.dropStream(testStreamName)
      }
    }
  }

  describe("The listen operation") {
    it("should return the stream flow") {
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
      val secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING)
      val columnList = Seq(firstStreamColumn, secondStreamColumn)
      val firstColumnValue = new ColumnNameValue("column1", new Integer(1))
      val secondColumnValue = new ColumnNameValue("column2", "testValue")
      val streamData = Seq(firstColumnValue, secondColumnValue)
      try {
        streamingAPI.createStream(testStreamName, columnList)
        val streams = streamingAPI.listenStream(testStreamName)
        Thread.sleep(2000)
        streamingAPI.insertData(testStreamName, streamData)
        for (stream <- streams) {
          val firstColumn = stream.message.getColumns.get(0)
          firstColumn.getColumn should be("column1")
          firstColumn.getValue should be(1)
          firstColumn.getType should be("INT")
          val secondColumn = stream.message.getColumns.get(1)
          secondColumn.getColumn should be("column2")
          secondColumn.getValue should be("testValue")
          secondColumn.getType should be("STRING")
          break
        }
      } catch {
        case ssEx: StratioStreamingException => fail()
        case _ => assert(true)
      } finally {
        streamingAPI.stopListenStream(testStreamName)
      }
    }

    it("should throw a StratioEngineStatusException when streaming engine is not running") {
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
      val secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING)
      val columnList = Seq(firstStreamColumn, secondStreamColumn)
      val firstColumnValue = new ColumnNameValue("column1", new Integer(1))
      val secondColumnValue = new ColumnNameValue("column2", "testValue")
      val streamData = Seq(firstColumnValue, secondColumnValue)
      streamingAPI.createStream(testStreamName, columnList)
      removeEphemeralNode()
      Thread.sleep(1000)
      intercept [StratioEngineStatusException] {
        streamingAPI.listenStream(testStreamName)
      }
    }

    it("should throw a StratioAPISecurityException when listening to an internal stream") {
      intercept [StratioAPISecurityException] {
        streamingAPI.listenStream(internalTestStreamName)
      }
    }
  }

  describe("The stop listen operation") {
    it("should stop the stream flow") {
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
      val secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING)
      val columnList = Seq(firstStreamColumn, secondStreamColumn)
      val firstColumnValue = new ColumnNameValue("column1", new Integer(1))
      val secondColumnValue = new ColumnNameValue("column2", "testValue")
      val streamData = Seq(firstColumnValue, secondColumnValue)
      streamingAPI.createStream(testStreamName, columnList)
      val streams = streamingAPI.listenStream(testStreamName)
      Thread.sleep(2000)
      streamingAPI.insertData(testStreamName, streamData)
      streamingAPI.stopListenStream(testStreamName)
      for (stream <- streams) {
        fail()
      }
      assert(true)
    }

    it("should throw a StratioEngineStatusException when streaming engine is not running") {
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
      val secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING)
      val columnList = Seq(firstStreamColumn, secondStreamColumn)
      streamingAPI.createStream(testStreamName, columnList)
      streamingAPI.listenStream(testStreamName)
      removeEphemeralNode()
      Thread.sleep(1000)
      intercept [StratioEngineStatusException] {
        streamingAPI.stopListenStream(testStreamName)
      }
    }

    it("should throw a StratioAPISecurityException when stop listening to an internal status") {
      intercept [StratioAPISecurityException] {
        streamingAPI.stopListenStream(internalTestStreamName)
      }
    }
  }

  describe("The list operation") {
    it("should throw a StratioEngineStatusException when streaming engine is not running") {
      removeEphemeralNode()
      Thread.sleep(1000)
      intercept[StratioEngineStatusException] {
        streamingAPI.listStreams()
      }
    }
  }

  def removeEphemeralNode() {
    zookeeperConsumer.removeZNode(ZK_EPHEMERAL_NODE_PATH)
  }

  def cleanStratioStreamingEngine() {
    userDefinedStreams.foreach(stream => streamingAPI.dropStream(stream.getStreamName))
  }

  def userDefinedStreams() = {
    streamingAPI.listStreams().filterNot(stream => stream.getStreamName.startsWith("stratio_"))
  }

  def theNumberOfUserDefinedStreams() = {
    userDefinedStreams.size
  }

  def theNumberOfColumnsOfTheStream(streamName: String) = {
    streamingAPI.columnsFromStream(streamName).size
  }

  def theNumberOfQueriesOfTheStream(streamName: String) = {
    streamingAPI.queriesFromStream(streamName).size
  }

}
