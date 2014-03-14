package com.stratio.bus

import scala.concurrent.{ExecutionContext, Future}
import com.netflix.curator.framework.CuratorFramework
import org.apache.zookeeper.data.Stat
import ExecutionContext.Implicits.global

case class ZookeeperConsumer(zooKeeperClient: CuratorFramework) {

  def readZNode(zNodeName: String) = {
    Future {
      var zNodeExists = checkZNodeExists(zNodeName)
      while(!zNodeHasBeenCreated(zNodeExists)) {
        zNodeExists = checkZNodeExists(zNodeName)
      }
    }
  }

  private def checkZNodeExists(zNodeName: String) = zooKeeperClient.checkExists().forPath(s"/$zNodeName")

  private def zNodeHasBeenCreated(zNode: Stat) = zNode != null

}
