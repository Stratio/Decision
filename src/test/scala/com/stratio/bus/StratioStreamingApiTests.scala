package com.stratio.bus

import org.scalatest._
import com.stratio.streaming.commons.messages.StratioStreamingMessage
import com.stratio.streaming.commons.exceptions.{StratioStreamingException, StratioEngineStatusException, StratioAPISecurityException}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.framework.CuratorFrameworkFactory
import com.stratio.bus.zookeeper.ZookeeperConsumer
import com.stratio.streaming.commons.constants.STREAMING._
import com.stratio.bus.zookeeper.ZookeeperConsumer

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
        StratioBusFactory.create().initialize()
      }
    }

    it("should throw a SecurityException when the user tries to perform an operation in an internal stream") {
      createEngineEphemeralNode()
      val internalStreamName = "stratio_whatever"
      val message = new StratioStreamingMessage()
      message.setStreamName(internalStreamName)
      val streamingAPI = StratioBusFactory.create().initialize()
      intercept [StratioAPISecurityException] {
        streamingAPI.send(message)
      }
    }

    it("should throw a StratioStreamingException when the API receives an unknown operation") {
      createEngineEphemeralNode()
      val streamName = "whatever"
      val operation = "unknownOperation"
      val message = new StratioStreamingMessage()
      message.setStreamName(streamName)
      message.setOperation(operation)
      val streamingAPI = StratioBusFactory.create().initialize()
      intercept [StratioStreamingException] {
        streamingAPI.send(message)
      }
    }

    def createEngineEphemeralNode() {
      zookeeperClient.create().forPath(ZK_EPHEMERAL_NODE_PATH)
    }
  }
}
