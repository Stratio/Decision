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
package com.stratio.decision.api.kafka

import com.google.gson._
import com.stratio.decision.commons.constants.ColumnType
import com.stratio.decision.commons.messages.ColumnNameTypeValue
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.lang.reflect.Type

class ColumnNameTypeValueDeserializer extends JsonDeserializer[ColumnNameTypeValue] {

  private val log: Logger = LoggerFactory.getLogger(classOf[ColumnNameTypeValueDeserializer])

  private val COLUMN_FIELD: String = "column"
  private val TYPE_FIELD: String = "type"
  private val VALUE_FIELD: String = "value"

  @Override
  def deserialize(element: JsonElement, _type: Type, ctx: JsonDeserializationContext): ColumnNameTypeValue = {
    
    val object_ = element.getAsJsonObject
    var name: String = None.orNull
    var columnType: ColumnType = None.orNull
    var value: Any = None.orNull

    if(object_ != None.orNull && object_.has(COLUMN_FIELD) && object_.has(TYPE_FIELD)) {
      name = object_.get(COLUMN_FIELD).getAsString
      columnType = ColumnType.valueOf(object_.get(TYPE_FIELD).getAsString)

      if (object_.has(VALUE_FIELD)) {
        val jsonValue: JsonElement = object_.get(VALUE_FIELD)
        columnType match {
          case ColumnType.BOOLEAN =>
            value = jsonValue.getAsBoolean
          case ColumnType.DOUBLE =>
              value = jsonValue.getAsDouble
          case ColumnType.FLOAT =>
              value = jsonValue.getAsFloat
          case ColumnType.INTEGER =>
              value = jsonValue.getAsInt
          case ColumnType.LONG =>
              value = jsonValue.getAsLong
          case ColumnType.STRING =>
              value = jsonValue.getAsString
          case _ => ()
        }
      } else log.warn("Column with name {} has no value", name)

      if (log.isDebugEnabled)
        log.debug(s"Values obtained into ColumnNameTypeValue deserialization: " +
          "NAME: $name, VALUE: $value, COLUMNTYPE: $columnType")

    } else log.warn("Error deserializing ColumnNameTypeValue from json. JsonObject is not complete: {}", element)

    new ColumnNameTypeValue(name, columnType, value)
  }
}
