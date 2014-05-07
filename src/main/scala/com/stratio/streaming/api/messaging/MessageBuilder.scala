/*
 * Copyright 2014 Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stratio.streaming.messaging

import com.stratio.streaming.commons.messages.{StreamQuery, ColumnNameTypeValue, StratioStreamingMessage}
import java.util.List

object MessageBuilder {

  class StratioStreamingMessageBuilder(theOperation: String,
                                        theStreamName: String,
                                        theSessionId: String,
                                        theRequestId: String,
                                        theRequest: String,
                                        theTimeStamp: Long,
                                        theColumns: List[ColumnNameTypeValue],
                                        theQueries: List[StreamQuery],
                                        theUserDefined: Boolean
                                        ) {

    def build() = new StratioStreamingMessage(theOperation,
                                              theStreamName,
                                              theSessionId,
                                              theRequestId,
                                              theRequest,
                                              theTimeStamp,
                                              theColumns,
                                              theQueries,
                                              theUserDefined)

    def withColumns(columns: List[ColumnNameTypeValue]) =
      new StratioStreamingMessageBuilder(theOperation,
         theStreamName,
         theSessionId,
         theRequestId,
         theRequest,
         theTimeStamp,
         columns,
         theQueries,
         theUserDefined)

    def withOperation(operation: String) =
      new StratioStreamingMessageBuilder(operation,
        theStreamName,
        theSessionId,
        theRequestId,
        theRequest,
        theTimeStamp,
        theColumns,
        theQueries,
        theUserDefined)

    def withStreamName(streamName: String) =
      new StratioStreamingMessageBuilder(theOperation,
        streamName,
        theSessionId,
        theRequestId,
        theRequest,
        theTimeStamp,
        theColumns,
        theQueries,
        theUserDefined)

    def withSessionId(sessionId: String) =
      new StratioStreamingMessageBuilder(theOperation,
        theStreamName,
        sessionId,
        theRequestId,
        theRequest,
        theTimeStamp,
        theColumns,
        theQueries,
        theUserDefined)

    def withRequest(request: String) =
      new StratioStreamingMessageBuilder(theOperation,
        theStreamName,
        theSessionId,
        theRequestId,
        request,
        theTimeStamp,
        theColumns,
        theQueries,
        theUserDefined)
  }

  def builder = new StratioStreamingMessageBuilder("",
                                            "",
                                            "",
                                            "" + System.currentTimeMillis,
                                            "",
                                            new java.lang.Long(System.currentTimeMillis),
                                            null,
                                            null,
                                            true)

}
