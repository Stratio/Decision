package com.stratio.bus

import org.scalatest.{ShouldMatchers, FunSpec}
import com.stratio.streaming.commons.messages.StratioStreamingMessage
import com.stratio.streaming.commons.exceptions.{StratioEngineStatusException, StratioAPISecurityException}

class StratioStreamingApiTests
  extends FunSpec
  with ShouldMatchers {

  describe("The Stratio Streaming API") {
    it("should throw a StratioEngineStatusException when the streaming engine is not running") {
      intercept [StratioEngineStatusException] {
        StratioBusFactory.create().initialize()
      }
    }

    /*
    it("should throw a SecurityException when the user tries to perform an operation in an internal stream") {
      val internalStreamName = "stratio_whatever"
      val message = new StratioStreamingMessage()
      message.setStreamName(internalStreamName)
      val streamingAPI = StratioBusFactory.create().initialize()
      intercept [StratioAPISecurityException] {
        streamingAPI.send(message)
      }
    } */
  }
}
