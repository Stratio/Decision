package com.stratio.bus

import org.scalatest.{GivenWhenThen, ShouldMatchers, FunSpec}
import com.netflix.curator.framework.CuratorFrameworkFactory
import com.netflix.curator.retry.ExponentialBackoffRetry
import scala.concurrent._
import scala.concurrent.duration._

class ZookeperConsumerTests
  extends FunSpec
  with ShouldMatchers
  with GivenWhenThen {

  val zookeeperCluster = "localhost:2181"
  val retryPolicy = new ExponentialBackoffRetry(1000, 3)
  val zookeeperClient = CuratorFrameworkFactory.newClient(zookeeperCluster, retryPolicy)
  zookeeperClient.start()
  val zookeeperConsumer = new ZookeeperConsumer(zookeeperClient)

  describe("The zookeeper consumer") {
    it("should not throw a TimeOutException when the znode has been created within the timeout") {
      /*
      Given("zookeper does not contain a znode")
      val zNodeName = "test_znode"
      val zNodeFullPath = zookeeperFullPath(zNodeName)
      if (zookeeperClient.checkExists().forPath(zNodeFullPath)!=null)
        zookeeperClient.delete().forPath(zNodeFullPath)
      When("I create the znode within the timeout")
      //zookeeperClient.create().forPath(zNodeFullPath,"test_data".getBytes())
      And("I call the readZNode method")
      intercept[TimeoutException] {
        zookeeperConsumer.readZNode(zNodeName)
      }
      Then("the TimeOutException should not be thrown")*/
    }

    it("should throw a TimeOutException when the znode has not been created within the timeout") {
      Given("zookeper does not contain a znode")
      When("I create a new Znode")
      Then("The znode should be created")
    }
  }

  def zookeeperFullPath(zNode: String) = s"/$zNode"

}
