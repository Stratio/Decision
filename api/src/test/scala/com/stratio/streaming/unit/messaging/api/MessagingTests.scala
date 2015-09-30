package com.stratio.streaming.unit.messaging.api

import com.stratio.streaming.api.messaging.{ColumnNameType, MessageBuilder, MessageBuilderWithColumns}
import com.stratio.streaming.commons.constants.ColumnType
import com.stratio.streaming.commons.messages.ColumnNameTypeValue
import org.junit.runner.RunWith
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner

import scala.collection.JavaConversions._


/**
 * Created by eruiz on 29/09/15.
 */

@RunWith(classOf[JUnitRunner])
class MessagingTests extends WordSpec {
  /*
  describe("When get the type") {
    it("should give the type in a column"){
      val columnNameType = new ColumnNameType("column1", ColumnType.STRING)
      assert(columnNameType.columnType == (ColumnType.STRING))
    }
  }
  describe("When get the value") {
    it("should give the value in a column"){
      val columnNameValue = new ColumnNameValue("column1", ColumnType.STRING)
      assert(columnNameValue.columnValue == (ColumnType.STRING))
    }
  }*/
  trait MyTestComponent {
    var m = MessageBuilder
    var mw = MessageBuilderWithColumns
  }

  "When build a MessageBuilder" should {
    "test-1" in new MyTestComponent {
      assert(m.builder.build().getOperation=="")
    }
    "test-2" in new MyTestComponent{
      val p = m.builder.withOperation("Operation")
      assert(p.build().getOperation=="Operation" )
    }
    "test-3" in new MyTestComponent{
      val p = m.builder.withStreamName("StreamName")
      assert(p.build().getStreamName=="StreamName" )
    }
    "test-4" in new MyTestComponent{
      val p = m.builder.withSessionId("SessionID")
      assert(p.build().getSession_id=="SessionID" )
    }
    "test-5" in new MyTestComponent{
      val p = m.builder.withRequest("Request")
      assert(p.build().getRequest=="Request" )
    }
    "test-6" in new MyTestComponent{
      val columnNameType: ColumnNameTypeValue = new ColumnNameTypeValue("column1", ColumnType.STRING, null)
      val p = m.builder.withColumns(List(columnNameType))
      assert(p.build().getColumns.get(0).equals(columnNameType) )
    }
    "test-7" in new MyTestComponent{
      val columnNameType: ColumnNameType = new ColumnNameType("column1", ColumnType.STRING)
      val p = mw.apply("sessionID","operation").build("streamName",List(columnNameType))
      assert(p.getSession_id == "sessionID" )
      assert(p.getOperation == "operation" )
      assert(p.getStreamName == "streamName" )
    }
  }

}
