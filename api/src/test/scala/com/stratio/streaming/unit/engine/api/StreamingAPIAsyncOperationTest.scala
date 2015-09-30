package com.stratio.streaming.unit.engine.api

import com.stratio.streaming.api.StreamingAPIAsyncOperation
import com.stratio.streaming.commons.constants.ReplyCode._
import com.stratio.streaming.commons.messages.StratioStreamingMessage
import com.stratio.streaming.kafka.KafkaProducer
import org.mockito.Matchers.anyString
import org.mockito.Mockito
import org.scalatest._
import org.scalatest.mock.MockitoSugar

/**
 * Created by eruiz on 30/09/15.
 */

class StreamingAPIAsyncOperationTest extends FunSpec
with GivenWhenThen
with ShouldMatchers
with MockitoSugar {
  val kafkaProducerMock = mock[KafkaProducer]
  val stratioStreamingAPIAsyncOperation = new StreamingAPIAsyncOperation(kafkaProducerMock)
  val stratioStreamingMessage = new StratioStreamingMessage()

  describe("The Streaming API Async Operation") {
    it("should throw no exceptions when the engine returns an OK return code") {
      Given("an OK engine response")
      val errorCode = OK.getCode();
      val engineResponse = s"""{"errorCode":$errorCode}"""
      When("we perform the async operation")
      Mockito.doNothing().when(kafkaProducerMock).send(anyString(), anyString())
      Then("we should not get a StratioAPISecurityException")
      try {
        stratioStreamingAPIAsyncOperation.performAsyncOperation(stratioStreamingMessage)
      } catch {
        case _ => fail()
      }
    }
  }
}
