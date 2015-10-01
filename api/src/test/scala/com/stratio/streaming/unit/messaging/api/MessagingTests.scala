package com.stratio.streaming.unit.messaging.api

import com.stratio.streaming.api.messaging.{ColumnNameType, MessageBuilderWithColumns, MessageBuilder}
import com.stratio.streaming.commons.constants.ColumnType
import com.stratio.streaming.commons.messages.ColumnNameTypeValue
import org.junit.runner.RunWith
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner
import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class MessagingTests extends WordSpec{
  trait MyTestComponent {
    var m = MessageBuilder
    var mw = MessageBuilderWithColumns
  }

  "When build a MessageBuilder" should {
    "be operation empty by default" in new MyTestComponent {
      assert(m.builder.build().getOperation=="")
    }
    "be Operation" in new MyTestComponent{
      val p = m.builder.withOperation("Operation")
      assert(p.build().getOperation=="Operation" )
    }
    "be StreamName" in new MyTestComponent{
      val p = m.builder.withStreamName("StreamName")
      assert(p.build().getStreamName=="StreamName" )
    }
    "be SessionID" in new MyTestComponent{
      val p = m.builder.withSessionId("SessionID")
      assert(p.build().getSession_id=="SessionID" )
    }
    "be Request" in new MyTestComponent{
      val p = m.builder.withRequest("Request")
      assert(p.build().getRequest=="Request" )
    }
    "be equal at the columnTypeValue parametres created" in new MyTestComponent{
      val columnNameType: ColumnNameTypeValue = new ColumnNameTypeValue("column1", ColumnType.STRING, null)
      val p = m.builder.withColumns(List(columnNameType))
      assert(p.build().getColumns.get(0).equals(columnNameType) )
    }
    "be equal at the columnTypeValue parametres created when apply" in new MyTestComponent{
      val columnNameType: ColumnNameType = new ColumnNameType("column1", ColumnType.STRING)
      val p = mw.apply("sessionID","operation").build("streamName",List(columnNameType))
      assert(p.getSession_id == "sessionID" )
      assert(p.getOperation == "operation" )
      assert(p.getStreamName == "streamName" )
    }
  }
}
