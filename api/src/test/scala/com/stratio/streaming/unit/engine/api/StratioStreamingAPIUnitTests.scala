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
package com.stratio.streaming.unit.engine.api

import com.stratio.streaming.api.StratioStreamingAPI
import com.stratio.streaming.commons.exceptions.StratioEngineStatusException
import org.scalatest._

class StratioStreamingAPIUnitTests
  extends WordSpec
  with ShouldMatchers {

  trait DummyStratioStreamingAPI {
    val api = new StratioStreamingAPI()
    api.streamingUp = false
    api.streamingRunning = false
  }

  "The Stratio Streaming API" when {
    /*"checks the status of streaming" should {

      "no throws any exception" in new DummyStratioStreamingAPI {
        api.streamingUp = true
        api.streamingRunning = true

        api.checkStreamingStatus() should equal(())

      }

      "throws an exception if streaming is down" in new DummyStratioStreamingAPI {
        api.streamingUp = false

        val thrown = intercept[StratioEngineStatusException] {
          api.checkStreamingStatus()
        }

        thrown.getMessage should be("Stratio streaming is down")
      }

      "throws an exception if streaming is not running" in new DummyStratioStreamingAPI {
        api.streamingUp = true
        api.streamingRunning = false

        val thrown = intercept[StratioEngineStatusException] {
          api.checkStreamingStatus()
        }

        thrown.getMessage should be("Stratio streaming not yet initialized")
      }
    }*/

    "set the server config" should {
      "modifies the server config values" in new DummyStratioStreamingAPI {
        api.kafkaCluster = ""
        api.zookeeperServer = ""

        val newKafkaCluster = "kafkaQuorumTest"
        val newZookeeperServer = "zookeeperQuorumTest"

        val result = api.withServerConfig(newKafkaCluster, newZookeeperServer)

        api.kafkaCluster should be(newKafkaCluster)
        api.zookeeperServer should be(newZookeeperServer)
      }
    }

    "set the server config (with ports)" should {
      "modifies the server config values" in new DummyStratioStreamingAPI {
        api.kafkaCluster = ""
        api.zookeeperServer = ""

        val newKafkaCluster = "kafkaQuorumTest:0"
        val newKafkaHost = "kafkaQuorumTest"
        val newKafkaPort = 0
        val newZookeeperServer = "zookeeperQuorumTest:0"
        val newZookeeperHost = "zookeeperQuorumTest"
        val newZookeeperPort = 0

        val result =
          api.withServerConfig(newKafkaHost, newKafkaPort, newZookeeperHost, newZookeeperPort)

        api.kafkaCluster should be(newKafkaCluster)
        api.zookeeperServer should be(newZookeeperServer)
      }
    }
  }
}
