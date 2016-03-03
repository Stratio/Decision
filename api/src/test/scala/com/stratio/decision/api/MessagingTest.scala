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

import com.stratio.decision.api.messaging.{ColumnNameType, MessageBuilderWithColumns, MessageBuilder}
import com.stratio.decision.commons.constants.ColumnType
import com.stratio.decision.commons.messages.ColumnNameTypeValue
import org.junit.runner.RunWith
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner
import scala.collection.JavaConversions._


@RunWith(classOf[JUnitRunner])
class MessagingTest extends WordSpec {
  trait MyTestComponent {
    var m = MessageBuilder
    var mw = new MessageBuilderWithColumns("sessionID","operation")
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
      val p = mw.build("streamName",List(columnNameType))
      assert(p.getSession_id == "sessionID" )
      assert(p.getOperation == "operation" )
      assert(p.getStreamName == "streamName" )
    }
  }
}
