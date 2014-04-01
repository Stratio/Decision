package com.stratio.bus.zookeeper

import scala.concurrent.{ExecutionContext, Future}
import ExecutionContext.Implicits.global
import com.typesafe.config.ConfigFactory
import org.apache.curator.framework.CuratorFramework
import scala.Predef._
import scala.Some

case class ZookeeperConsumer(zooKeeperClient: CuratorFramework) {
  val config = ConfigFactory.load()

  def readZNode(fullPath: String) = {
    Future {
      var nodeExists = zNodeExists(fullPath)
      while(!nodeExists) {
        nodeExists = zNodeExists(fullPath)
      }
    }
  }

  def getZNodeData(fullPath: String): Option[String] = {
    zNodeExists(fullPath) match {
      case true => Some(new String(zooKeeperClient.getData.forPath(fullPath)))
      case _ => None
    }
  }

  def removeZNode(fullPath: String) = {
    if (zNodeExists(fullPath))
      zooKeeperClient.delete.forPath(fullPath)
  }

  def zNodeExists(zNodeName: String) = {
    val zNode = checkZNode(zNodeName)
    zNode != null
  }

  private def checkZNode(zNodeName: String) = zooKeeperClient.checkExists().forPath(zNodeName)

  //private def zNodeExists(zNode: Stat) = zNode != null
}
