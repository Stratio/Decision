/*
 * Copyright 2014 Stratio Big Data, S.L.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stratio.streaming.zookeeper

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
