package com.stratio.streaming.unit

import org.scalatest._
import com.stratio.streaming.kafka.JsonGenericDecoder

class JsonGenericDecoderTests
  extends FunSpec
  with ShouldMatchers {

  describe("The json generic decoder") {
    it("should decode a json into a specific class") {
      class TestClass(var field1: String, var field2: Integer)
      val jsonToBeParsed = """{"operation":"insert","streamName":"testStream","session_id":"1396942951802","request_id":"1396942953315","request":"","timestamp":1396942953315,"columns":[{"column":"field1","value":"testString"},{"column":"field2","value":200}],"userDefined":true}"""

      val jsonGenericDecoder = new JsonGenericDecoder

      val fieldsList = jsonGenericDecoder.fromBytes(jsonToBeParsed.getBytes())
      fieldsList.getColumns.size should be(2)
      fieldsList.getOperation should be("insert")
      fieldsList.getStreamName should be ("testStream")
    }

    it("should throw a StratioAPIParserException when the parser is not able to parse the json") {

    }
  }
}
