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
package com.stratio.streaming.unit

import org.scalatest._
import com.stratio.streaming.commons.messages.StratioStreamingMessage
import com.stratio.streaming.commons.exceptions.{StratioAPIGenericException, StratioEngineOperationException, StratioAPISecurityException}
import org.scalatest.mock._
import org.mockito.Matchers._
import scala.collection.JavaConversions._
import org.mockito.Mockito
import scala.concurrent.Future
import com.stratio.streaming.commons.constants.REPLY_CODES._
import com.stratio.streaming.kafka.KafkaProducer
import com.stratio.streaming.api.StreamingAPISyncOperation
import com.stratio.streaming.zookeeper.ZookeeperConsumer
import scala.Some

class StreamingAPISyncOperationTests extends FunSpec
with GivenWhenThen
with ShouldMatchers
with MockitoSugar {
  val kafkaProducerMock = mock[KafkaProducer]
  val zookeeperConsumerMock = mock[ZookeeperConsumer]
  val stratioStreamingAPISyncOperation = new StreamingAPISyncOperation(kafkaProducerMock, zookeeperConsumerMock, 2000)
  val stratioStreamingMessage = new StratioStreamingMessage(
    "theOperation",
    "theStreamName",
    "sessionId",
    "requestId",
    "theRequest",
    123456,
    Seq(),
    Seq(),
    true)

  describe("The Streaming API Sync Operation") {
    it("should throw no exceptions when the engine returns an OK return code") {
      Given("an OK engine response")
      val engineResponse = s"""{"errorCode":$OK}"""
      When("we perform the sync operation")
      Mockito.doNothing().when(kafkaProducerMock).send(anyString(), anyString())
      org.mockito.Mockito.when(zookeeperConsumerMock.zNodeExists(anyString())).thenReturn(true)
      org.mockito.Mockito.when(zookeeperConsumerMock.readZNode(anyString())).thenReturn(Future.successful())
      org.mockito.Mockito.when(zookeeperConsumerMock.getZNodeData(anyString())).thenReturn(Some(engineResponse))
      Then("we should not get a StratioAPISecurityException")
      try {
        stratioStreamingAPISyncOperation.performSyncOperation(stratioStreamingMessage)
      } catch {
        case _ => fail()
      }
    }

    it("should throw a StratioAPISecurityException when the engine returns a KO_STREAM_OPERATION_NOT_ALLOWED return code") {
      Given("a KO_STREAM_OPERATION_NOT_ALLOWED engine response")
      val engineResponse = s"""{"errorCode":$KO_STREAM_OPERATION_NOT_ALLOWED}"""
      When("we perform the sync operation")
      Mockito.doNothing().when(kafkaProducerMock).send(anyString(), anyString())
      org.mockito.Mockito.when(zookeeperConsumerMock.zNodeExists(anyString())).thenReturn(true)
      org.mockito.Mockito.when(zookeeperConsumerMock.readZNode(anyString())).thenReturn(Future.successful())
      org.mockito.Mockito.when(zookeeperConsumerMock.getZNodeData(anyString())).thenReturn(Some(engineResponse))
      Then("we should get a StratioAPISecurityException")
      intercept[StratioAPISecurityException] {
        stratioStreamingAPISyncOperation.performSyncOperation(stratioStreamingMessage)
      }
    }

    it("should throw a StratioAPISecurityException when the engine returns a KO_STREAM_IS_NOT_USER_DEFINED return code") {
        Given("a KO_STREAM_IS_NOT_USER_DEFINED engine response")
        val engineResponse = s"""{"errorCode":$KO_STREAM_IS_NOT_USER_DEFINED}"""
        When("we perform the sync operation")
        Mockito.doNothing().when(kafkaProducerMock).send(anyString(), anyString())
        org.mockito.Mockito.when(zookeeperConsumerMock.zNodeExists(anyString())).thenReturn(true)
        org.mockito.Mockito.when(zookeeperConsumerMock.readZNode(anyString())).thenReturn(Future.successful())
        org.mockito.Mockito.when(zookeeperConsumerMock.getZNodeData(anyString())).thenReturn(Some(engineResponse))
        Then("we should get a StratioAPISecurityException")
        intercept[StratioAPISecurityException] {
          stratioStreamingAPISyncOperation.performSyncOperation(stratioStreamingMessage)
        }
    }

    it("should throw a StratioEngineOperationException when the engine returns an ERROR return code") {
      Given("a KO_STREAM_IS_NOT_USER_DEFINED engine response")
      val engineResponse = s"""{"errorCode":$KO_COLUMN_DOES_NOT_EXIST}"""
      When("we perform the sync operation")
      Mockito.doNothing().when(kafkaProducerMock).send(anyString(), anyString())
      org.mockito.Mockito.when(zookeeperConsumerMock.zNodeExists(anyString())).thenReturn(true)
      org.mockito.Mockito.when(zookeeperConsumerMock.readZNode(anyString())).thenReturn(Future.successful())
      org.mockito.Mockito.when(zookeeperConsumerMock.getZNodeData(anyString())).thenReturn(Some(engineResponse))
      Then("we should get a StratioEngineOperationException")
      intercept[StratioEngineOperationException] {
        stratioStreamingAPISyncOperation.performSyncOperation(stratioStreamingMessage)
      }
    }

    it("should throw a StratioAPIGenericException when the response is a not well-formed json") {
      Given("a not well-formed engine response")
      val engineResponse = s"""{not well-formed json}"""
      When("we perform the sync operation")
      Mockito.doNothing().when(kafkaProducerMock).send(anyString(), anyString())
      org.mockito.Mockito.when(zookeeperConsumerMock.zNodeExists(anyString())).thenReturn(true)
      org.mockito.Mockito.when(zookeeperConsumerMock.readZNode(anyString())).thenReturn(Future.successful())
      org.mockito.Mockito.when(zookeeperConsumerMock.getZNodeData(anyString())).thenReturn(Some(engineResponse))
      Then("we should not get a StratioAPIGenericException")
      intercept[StratioAPIGenericException] {
        stratioStreamingAPISyncOperation.performSyncOperation(stratioStreamingMessage)
      }
    }

    it("should throw a StratioAPIGenericException when the API is not able to parse de response") {
      Given("an unknown engine response")
      val engineResponse = s"""{"unknownField": "blah"}"""
      When("we perform the sync operation")
      Mockito.doNothing().when(kafkaProducerMock).send(anyString(), anyString())
      org.mockito.Mockito.when(zookeeperConsumerMock.zNodeExists(anyString())).thenReturn(true)
      org.mockito.Mockito.when(zookeeperConsumerMock.readZNode(anyString())).thenReturn(Future.successful())
      org.mockito.Mockito.when(zookeeperConsumerMock.getZNodeData(anyString())).thenReturn(Some(engineResponse))
      Then("we should not get a StratioAPIGenericException")
      intercept[StratioAPIGenericException] {
        stratioStreamingAPISyncOperation.performSyncOperation(stratioStreamingMessage)
      }
    }
  }
}
