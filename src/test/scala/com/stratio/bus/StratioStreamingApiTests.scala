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

}
