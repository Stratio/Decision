package com.stratio.bus

import org.scalatest._
import com.stratio.streaming.commons.exceptions.{StratioEngineOperationException, StratioStreamingException, StratioEngineStatusException, StratioAPISecurityException}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.framework.CuratorFrameworkFactory
import com.stratio.streaming.commons.constants.STREAMING._
import com.stratio.bus.zookeeper.ZookeeperConsumer
import com.stratio.bus.messaging.ColumnNameType
import com.stratio.streaming.commons.constants.ColumnType
import scala.collection.JavaConversions._

class StratioStreamingApiTests
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
    if (!zookeeperConsumer.zNodeExists(ZK_EPHEMERAL_NODE_PATH))
      zookeeperClient.create().forPath(ZK_EPHEMERAL_NODE_PATH)
    cleanStratioStreamingEngine()
  }

  describe("The create operation") {
    it("should create a new stream when the stream does not exist") {
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
      val secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING)
      val columnList = Seq(firstStreamColumn, secondStreamColumn)
      try {
        streamingAPI.createStream(testStreamName, columnList)
      } catch {
        case ssEx: StratioStreamingException => fail()
      }

      theNumberOfUserDefinedStreams should be(1)
      theNumberOfColumnsOfStream(testStreamName) should be(2)
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
      theNumberOfColumnsOfStream(testStreamName) should be(3)
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

  // TODO
  // describe("The insert operation")

  describe("The add query operation") {
    it("should add a new query to an existing stream") {
      val alarmsStream = "alarms"
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
      val secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING)
      val columnList = Seq(firstStreamColumn, secondStreamColumn)
      val theQuery = s"from $testStreamName select column1, column2 insert into alarms for current-events"
      try {
        streamingAPI.createStream(alarmsStream, columnList)
        streamingAPI.createStream(testStreamName, columnList)
        streamingAPI.addQuery(testStreamName, theQuery)
      } catch {
        case ssEx: StratioStreamingException => fail()
      }

      theNumberOfQueriesOfStream(testStreamName) should be(1)
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
      Thread.sleep(1000)
      intercept [StratioEngineStatusException] {
        streamingAPI.addQuery(testStreamName, theQuery)
      }
    }

    it("should throw a StratioAPISecurityException when adding a query to a non user-defined stream", Tag("wip")) {
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

  def theNumberOfColumnsOfStream(streamName: String) = {
    streamingAPI.columnsFromStream(streamName).size
  }

  def theNumberOfQueriesOfStream(streamName: String) = {
    streamingAPI.queriesFromStream(streamName).size
  }

}
