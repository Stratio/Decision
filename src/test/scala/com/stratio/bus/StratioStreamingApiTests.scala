package com.stratio.bus

import org.scalatest._
import com.stratio.streaming.commons.exceptions.{StratioEngineStatusException, StratioAPISecurityException}
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

  override def beforeEach() {
    zookeeperConsumer.removeZNode(ZK_EPHEMERAL_NODE_PATH)
  }

  describe("The Stratio Streaming API") {
    it("should throw a StratioEngineStatusException when the streaming engine is not running") {
      intercept [StratioEngineStatusException] {
        StratioStreamingAPIFactory.create().initialize()
      }
    }

    it("should throw a SecurityException when the user tries to perform an operation in an internal stream") {
      createEngineEphemeralNode()
      val internalStreamName = "stratio_whatever"
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
      val secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING)
      val columnList = Seq(firstStreamColumn, secondStreamColumn)
      val streamingAPI = StratioStreamingAPIFactory.create().initialize()
      intercept [StratioAPISecurityException] {
        streamingAPI.createStream(internalStreamName, columnList)
      }
    }

    def createEngineEphemeralNode() {
      zookeeperClient.create().forPath(ZK_EPHEMERAL_NODE_PATH)
    }
  }
}
