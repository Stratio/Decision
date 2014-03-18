package com.stratio.bus

import org.scalatest.{ShouldMatchers, FunSpec}
import StreamingAckValues._

class StreamAckValuesTests
  extends FunSpec
  with ShouldMatchers {

  it("should match by AckOk") {
    val stringAckOk = "0"
    stringAckOk match {
      case StreamingAckValues(AckOk) => assert(true)
      case _ => fail()
    }
  }

  it("should match by AckError") {
    val stringAckOk = "1"
    stringAckOk match {
      case StreamingAckValues(AckError) => assert(true)
      case _ => fail()
    }
  }

}
