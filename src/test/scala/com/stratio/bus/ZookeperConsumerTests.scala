package com.stratio.bus

import org.scalatest.{BeforeAndAfterAll, GivenWhenThen, ShouldMatchers, FunSpec}
import com.netflix.curator.framework.CuratorFrameworkFactory
import com.netflix.curator.retry.ExponentialBackoffRetry
import scala.concurrent._
import scala.concurrent.duration._
import java.util.UUID

class ZookeperConsumerTests
  extends FunSpec
  with ShouldMatchers
  with GivenWhenThen
  with BeforeAndAfterAll {

  val zookeeperCluster = "localhost:2181"
  val retryPolicy = new ExponentialBackoffRetry(1000, 3)
  val zookeeperClient = CuratorFrameworkFactory.newClient(zookeeperCluster, retryPolicy)
  zookeeperClient.start()
  val zookeeperConsumer = new ZookeeperConsumer(zookeeperClient)
  val operation = "theOperation"

  override def beforeAll() {
    createZookeeperFullPath()
  }

  describe("The zookeeper consumer") {
    it("should create the full listener path") {
      Given("a uniqueid and the operation")
      val uniqueId = UUID.randomUUID().toString
      When("i generate the path for a creation op")
      val fullPath = zookeeperConsumer.getZNodeFullPath(uniqueId, operation)
      Then("the path should be the full path")
      val expectedFullPath = s"/stratio/streaming/$operation/$uniqueId"
      expectedFullPath should be equals (fullPath)
    }


    it("should not throw a TimeOutException when the znode has been created within the timeout") {
      Given("a uniqueId and an operation")
      val uniqueId = UUID.randomUUID().toString
      val operation = "theOperation"
      val fullPath = zookeeperConsumer.getZNodeFullPath(uniqueId, operation)
      When("I create the znode within the timeout")
      zookeeperClient.create().forPath(fullPath)
      And("I call the readZNode method")
      Await.result(zookeeperConsumer.readZNode(uniqueId, operation), 1 seconds)
      Then("the TimeOutException should not be thrown")
    }


    it("should throw a TimeOutException when the znode has not been created within the timeout") {
      Given("a uniqueId and an operation")
      val uniqueId = UUID.randomUUID().toString
      val operation = "theOperation"
      When("I do not create the znode within the timeout")
      And("I call the readZNode method")
      Then("the TimeOutException should be thrown")
      intercept [TimeoutException] {
        Await.result(zookeeperConsumer.readZNode(uniqueId, operation), 1 seconds)
      }
    }
  }

  def createZookeeperFullPath() {
    if (zookeeperClient.checkExists().forPath("/stratio")==null) {
      zookeeperClient.create().forPath("/stratio")
    }
    if (zookeeperClient.checkExists().forPath("/stratio/streaming")==null) {
      zookeeperClient.create().forPath("/stratio/streaming")
    }
    if (zookeeperClient.checkExists().forPath(s"/stratio/streaming/$operation")==null) {
      zookeeperClient.create().forPath(s"/stratio/streaming/$operation")
    }
  }
}
