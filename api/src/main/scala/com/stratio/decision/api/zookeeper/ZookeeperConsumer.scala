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
package com.stratio.decision.api.zookeeper

import org.apache.curator.framework.CuratorFramework
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class ZookeeperConsumer(zooKeeperClient: CuratorFramework) {
  val log = LoggerFactory.getLogger(getClass)

  def readZNode(fullPath: String) = {
    Future {
      var nodeExists = zNodeExists(fullPath)
      while (!nodeExists) {
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
    checkZNode(zNodeName) match {
      case Some(true) => true
      case _ => false
    }
  }

  private def checkZNode(zNodeName: String) = {
    val zNodeStat = zooKeeperClient.checkExists().forPath(zNodeName)
    val zNodeExists = zNodeStat != null
    Some(zNodeExists)
  }
}
