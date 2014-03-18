package com.stratio.bus

import scala.concurrent.{ExecutionContext, Future}
import com.netflix.curator.framework.CuratorFramework
import org.apache.zookeeper.data.Stat
import ExecutionContext.Implicits.global
import com.typesafe.config.ConfigFactory
import scala.Predef.String

case class ZookeeperConsumer(zooKeeperClient: CuratorFramework) {
  val config = ConfigFactory.load()

  def readZNode(fullPath: String) = {
    Future {
      var zNode = checkZNode(fullPath)
      while(!zNodeExists(zNode)) {
        zNode = checkZNode(fullPath)
      }
    }
  }

  def getZNodeData(fullPath: String): Option[String] = {
    val zNode = checkZNode(fullPath)
    zNodeExists(zNode) match {
      case true => Some(new String(zooKeeperClient.getData.forPath(fullPath)))
      case _ => None
    }
  }

  def removeZNode(fullPath: String) = {
    val zNode = checkZNode(fullPath)
    if (zNodeExists(zNode))
      zooKeeperClient.delete.forPath(fullPath)
  }

  private def checkZNode(zNodeName: String) = zooKeeperClient.checkExists().forPath(zNodeName)

  private def zNodeExists(zNode: Stat) = zNode != null
}
