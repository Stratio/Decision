package com.stratio.bus

import scala.concurrent.{ExecutionContext, Future}
import com.netflix.curator.framework.CuratorFramework
import org.apache.zookeeper.data.Stat
import ExecutionContext.Implicits.global
import com.typesafe.config.ConfigFactory

case class ZookeeperConsumer(zooKeeperClient: CuratorFramework) {
  val config = ConfigFactory.load()

  def readZNode(fullPath: String) = {
    Future {
      var zNode = checkZNode(fullPath)
      while(!zNodeHasBeenCreated(zNode)) {
        zNode = checkZNode(fullPath)
      }
    }
  }

  private def checkZNode(zNodeName: String) = zooKeeperClient.checkExists().forPath(zNodeName)

  private def zNodeHasBeenCreated(zNode: Stat) = zNode != null
}
