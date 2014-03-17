package com.stratio.bus

import scala.concurrent.{ExecutionContext, Future}
import com.netflix.curator.framework.CuratorFramework
import org.apache.zookeeper.data.Stat
import ExecutionContext.Implicits.global
import com.typesafe.config.ConfigFactory

case class ZookeeperConsumer(zooKeeperClient: CuratorFramework) {
  val config = ConfigFactory.load()

  def readZNode(uniqueId: String, operation: String) = {
    val zNodeName = getZNodeFullPath(uniqueId, operation)
    Future {
      var zNode = checkZNode(zNodeName)
      while(!zNodeHasBeenCreated(zNode)) {
        zNode = checkZNode(zNodeName)
      }
    }
  }

  private def checkZNode(zNodeName: String) = zooKeeperClient.checkExists().forPath(zNodeName)

  private def zNodeHasBeenCreated(zNode: Stat) = zNode != null

  private def getZNodeFullPath(uniqueId: String, operation: String) = {
    val zookeeperPath = config.getString("zookeeper.listener.path")
    s"$zookeeperPath/$operation/$uniqueId"
  }

}
