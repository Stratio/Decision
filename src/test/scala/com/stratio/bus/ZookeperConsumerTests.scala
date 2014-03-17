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
  val operationFullPath = s"/stratio/streaming/$operation"

  override def beforeAll() {
    createZookeeperFullPath()
  }

  describe("The zookeeper consumer") {
    it("should not throw a TimeOutException when the znode has been created within the timeout") {
      Given("a uniqueId and an operation")
      val uniqueId = UUID.randomUUID().toString
      val fullPath = s"$operationFullPath/$uniqueId"
      When("I create the znode within the timeout")
      zookeeperClient.create().forPath(fullPath)
      And("I call the readZNode method")
      Await.result(zookeeperConsumer.readZNode(fullPath), 1 seconds)
      Then("the TimeOutException should not be thrown")
    }

    it("should throw a TimeOutException when the znode has not been created within the timeout") {
      Given("a uniqueId and an operation")
      val uniqueId = UUID.randomUUID().toString
      val fullPath = s"$operationFullPath/$uniqueId"
      When("I do not create the znode within the timeout")
      And("I call the readZNode method")
      Then("the TimeOutException should be thrown")
      intercept [TimeoutException] {
        Await.result(zookeeperConsumer.readZNode(fullPath), 1 seconds)
      }
    }

    it("should pick up the value from the zNode") {
      Given("a zNode with data")
      val uniqueId = UUID.randomUUID().toString
      val fullPath = s"$operationFullPath/$uniqueId"
      zookeeperClient.create().forPath(fullPath, "someData".getBytes())
      When("i fetch the value")
      val data = zookeeperConsumer.getZNodeData(fullPath)
      Then("i should get the data")
      data.get should be ("someData")
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
