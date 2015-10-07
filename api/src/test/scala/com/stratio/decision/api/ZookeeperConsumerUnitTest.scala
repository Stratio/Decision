/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.decision.api

import java.util.UUID

import com.netflix.curator.test.TestingServer
import com.stratio.decision.api.zookeeper.ZookeeperConsumer
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.RetryOneTime
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSpec, GivenWhenThen, ShouldMatchers}

import scala.concurrent._
import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class ZookeeperConsumerUnitTest
  extends FunSpec
  with ShouldMatchers
  with GivenWhenThen
  with BeforeAndAfterAll {

  val zookeeperCluster = "localhost:4711"
  val retryPolicy = new RetryOneTime(500)
  val zookeeperServer = new TestingServer(4711)
  //lazy val zookeeperClient = ZkTestSystem.createZkClient(zookeeperCluster)
  lazy val zookeeperClient = CuratorFrameworkFactory.newClient(zookeeperCluster, retryPolicy)
  zookeeperClient.start()
  lazy val zookeeperConsumer = new ZookeeperConsumer(zookeeperClient)
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
      intercept[TimeoutException] {
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
      data.isDefined should be(true)
      data.get should be("someData")
    }

    it("should return None when the zNode does not exist") {
      Given("a non existing zNode")
      val uniqueId = UUID.randomUUID().toString
      val fullPath = s"$operationFullPath/$uniqueId"
      When("i fetch the value")
      val data = zookeeperConsumer.getZNodeData(fullPath)
      Then("i should get None")
      data.isDefined should be(false)
    }

    it("should remove a zNode") {
      Given("an existing zNode")
      val uniqueId = UUID.randomUUID().toString
      val fullPath = s"$operationFullPath/$uniqueId"
      zookeeperClient.create().forPath(fullPath, "someData".getBytes())
      When("i remove the node")
      zookeeperConsumer.removeZNode(fullPath)
      Then("the node should be removed")
      theNodeExists(fullPath) should be(false)
    }

    it("should not throw an Exception when removing a non existing node") {
      Given("a non existing zNode")
      val uniqueId = UUID.randomUUID().toString
      val fullPath = s"$operationFullPath/$uniqueId"
      When("i remove a non existing node")
      zookeeperConsumer.removeZNode(fullPath)
      Then("the consumer should not throw an exception")
      //theNodeExists(fullPath) should be(false)
    }
  }

  private def theNodeExists(path: String): Boolean = {
    zookeeperClient.checkExists().forPath(path) != null
  }

  def createZookeeperFullPath() {
    if (!theNodeExists("/stratio")) {
      zookeeperClient.create().forPath("/stratio")
    }
    if (!theNodeExists("/stratio/streaming")) {
      zookeeperClient.create().forPath("/stratio/streaming")
    }
    if (!theNodeExists(s"/stratio/streaming/$operation")) {
      zookeeperClient.create().forPath(s"/stratio/streaming/$operation")
    }
  }
}
