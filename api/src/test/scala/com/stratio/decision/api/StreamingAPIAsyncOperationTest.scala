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

import com.stratio.decision.api.kafka.KafkaProducer
import com.stratio.decision.commons.avro.InsertMessage
import com.stratio.decision.commons.constants.ReplyCode._
import com.stratio.decision.commons.messages.StratioStreamingMessage
import org.junit.runner.RunWith
import org.mockito.Matchers.anyString
import org.mockito.Mockito
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.mockito.Matchers.any

@RunWith(classOf[JUnitRunner])
@Ignore
class StreamingAPIAsyncOperationTest extends FunSpec
with GivenWhenThen
with ShouldMatchers
with MockitoSugar {
  val kafkaProducerMock = mock[KafkaProducer]
  val stratioStreamingAPIAsyncOperation = new StreamingAPIAsyncOperation(kafkaProducerMock)
  val stratioStreamingMessage = new StratioStreamingMessage()

  describe("The Decision API Async Operation") {
    it("should throw no exceptions when the engine returns an OK return code") {
      Given("an OK engine response")
      val errorCode = OK.getCode
      val engineResponse = s"""{"errorCode":$errorCode}"""
      When("we perform the async operation")

      Mockito.doNothing().when(kafkaProducerMock).sendAvro(any(classOf[InsertMessage]),anyString())

      //def serializeInsertMessage(insertMessage: InsertMessage): Array[Byte]

     // Mockito.doNothing().when(kafkaProducerMock).sendAvro(any(classOf[InsertMessage]),anyString())

      Then("we should not get a StratioAPISecurityException")
      try {
        stratioStreamingAPIAsyncOperation.performAsyncOperation(stratioStreamingMessage)
      } catch {
        case _: Throwable => fail()
      }
    }
  }
}
