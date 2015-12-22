package com.stratio.decision.api.messaging

import com.stratio.decision.api.messaging.MessageBuilder._

/**
  * Created by josepablofernandez on 21/12/15.
  */
class DroolsMessageBuilde (sessionId: String){

  def build(streamName: String, operation: String) = {
    builder.withOperation(operation)
      .withStreamName(streamName)
      .withSessionId(sessionId)
      .build()
  }

}
