/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.decision.api.kafka

import com.google.gson.GsonBuilder
import com.stratio.decision.api.kafka.JsonGenericDecoder._
import com.stratio.decision.commons.exceptions.StratioAPIGenericException
import com.stratio.decision.commons.messages.{ColumnNameTypeValue, StratioStreamingMessage}
import kafka.serializer.Decoder

class JsonGenericDecoder extends Decoder[StratioStreamingMessage] {
  def fromBytes(bytes: Array[Byte]): StratioStreamingMessage = {
    try {
      jsonParser.fromJson(new String(bytes), classOf[StratioStreamingMessage])
    } catch {
      case _: Throwable => throw new StratioAPIGenericException("Decision API error: unable to decode the engine message")
    }
  }
}

object JsonGenericDecoder {
  private val jsonParser =
    new GsonBuilder()
      .registerTypeAdapter(classOf[ColumnNameTypeValue], new ColumnNameTypeValueDeserializer())
      .create()
}