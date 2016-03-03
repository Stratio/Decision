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

import com.stratio.decision.api.kafka.ColumnNameTypeValueDeserializer
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

import com.google.gson.JsonDeserializationContext
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import com.google.gson.JsonParser

import java.lang.reflect.Type

@RunWith(classOf[JUnitRunner])
class ColumnNameTypeValueDeserializerUnitTest
  extends WordSpec
  with ShouldMatchers
  with MockitoSugar {

  trait TestValues {

    val deserializer = new ColumnNameTypeValueDeserializer()
    val jsonElement = mock[JsonElement]
    val _type = mock[Type]
    val ctx = mock[JsonDeserializationContext]

    val COLUMN_NAME_TYPE_VALUE_STRING_EXAMPLE_OK =
      "{\"column\":\"test\",\"type\":\"STRING\",\"value\":\"testString\"}"

    val COLUMN_NAME_TYPE_VALUE_DOUBLE_EXAMPLE_OK =
      "{\"column\":\"test\",\"type\":\"DOUBLE\",\"value\":2}"

    val COLUMN_NAME_TYPE_VALUE_FLOAT_EXAMPLE_OK =
      "{\"column\":\"test\",\"type\":\"FLOAT\",\"value\":2.0}"

    val COLUMN_NAME_TYPE_VALUE_INTEGER_EXAMPLE_OK =
      "{\"column\":\"test\",\"type\":\"INTEGER\",\"value\":2}"

    val COLUMN_NAME_TYPE_VALUE_LONG_EXAMPLE_OK =
      "{\"column\":\"test\",\"type\":\"LONG\",\"value\":2}"

    val COLUMN_NAME_TYPE_VALUE_BOOLEAN_EXAMPLE_OK =
      "{\"column\":\"test\",\"type\":\"BOOLEAN\",\"value\":true}"

    val COLUMN_NAME_TYPE_VALUE_STRING_EXAMPLE_NO_VALUE_BUT_OK =
      "{\"column\":\"test\",\"type\":\"STRING\"}"

    val COLUMN_NAME_TYPE_VALUE_STRING_EXAMPLE_KO = "{}"

    def checkNullValue(value: Any) =
      assert(value == None.orNull, "Expected null but found a value")

    def checkNoNullValue(value: Any) =
      assert(value != None.orNull, "Expected value but found null")
  }

  "The column name type value deserializer" when {
    "the json is empty" in new TestValues {

      val parser = new JsonParser()
      val parsed: JsonObject = parser.parse(COLUMN_NAME_TYPE_VALUE_STRING_EXAMPLE_KO).asInstanceOf[JsonObject]
      when(jsonElement.getAsJsonObject).thenReturn(parsed)

      val result = deserializer.deserialize(jsonElement, _type, ctx)

      checkNullValue(result.getColumn)
      checkNullValue(result.getType)
      checkNullValue(result.getValue)
    }

    "the json object is null" in new TestValues {

      when(jsonElement.getAsJsonObject).thenReturn(None.orNull)

      val result = deserializer.deserialize(jsonElement, _type, ctx)

      checkNullValue(result.getColumn)
      checkNullValue(result.getType)
      checkNullValue(result.getValue)
    }

    "the json object has a string value" in new TestValues {

      val parser = new JsonParser()
      val parsed: JsonObject = parser.parse(COLUMN_NAME_TYPE_VALUE_STRING_EXAMPLE_OK).asInstanceOf[JsonObject]
      when(jsonElement.getAsJsonObject).thenReturn(parsed)

      val result = deserializer.deserialize(jsonElement, _type, ctx)

      checkNoNullValue(result.getColumn)
      checkNoNullValue(result.getType)
      checkNoNullValue(result.getValue)
    }

    "the json object has a double value" in new TestValues {

      val parser = new JsonParser()
      val parsed: JsonObject = parser.parse(COLUMN_NAME_TYPE_VALUE_DOUBLE_EXAMPLE_OK).asInstanceOf[JsonObject]
      when(jsonElement.getAsJsonObject).thenReturn(parsed)

      val result = deserializer.deserialize(jsonElement, _type, ctx)

      checkNoNullValue(result.getColumn)
      checkNoNullValue(result.getType)
      checkNoNullValue(result.getValue)
    }

    "the json object has a float value" in new TestValues {

      val parser = new JsonParser()
      val parsed: JsonObject = parser.parse(COLUMN_NAME_TYPE_VALUE_FLOAT_EXAMPLE_OK).asInstanceOf[JsonObject]
      when(jsonElement.getAsJsonObject).thenReturn(parsed)

      val result = deserializer.deserialize(jsonElement, _type, ctx)

      checkNoNullValue(result.getColumn)
      checkNoNullValue(result.getType)
      checkNoNullValue(result.getValue)
    }

    "the json object has a integer value" in new TestValues {

      val parser = new JsonParser()
      val parsed: JsonObject = parser.parse(COLUMN_NAME_TYPE_VALUE_INTEGER_EXAMPLE_OK).asInstanceOf[JsonObject]
      when(jsonElement.getAsJsonObject).thenReturn(parsed)

      val result = deserializer.deserialize(jsonElement, _type, ctx)

      checkNoNullValue(result.getColumn)
      checkNoNullValue(result.getType)
      checkNoNullValue(result.getValue)
    }

    "the json object has a long value" in new TestValues {

      val parser = new JsonParser()
      val parsed: JsonObject = parser.parse(COLUMN_NAME_TYPE_VALUE_LONG_EXAMPLE_OK).asInstanceOf[JsonObject]
      when(jsonElement.getAsJsonObject).thenReturn(parsed)

      val result = deserializer.deserialize(jsonElement, _type, ctx)

      checkNoNullValue(result.getColumn)
      checkNoNullValue(result.getType)
      checkNoNullValue(result.getValue)
    }

    "the json object has a boolean value" in new TestValues {

      val parser = new JsonParser()
      val parsed: JsonObject = parser.parse(COLUMN_NAME_TYPE_VALUE_BOOLEAN_EXAMPLE_OK).asInstanceOf[JsonObject]
      when(jsonElement.getAsJsonObject).thenReturn(parsed)

      val result = deserializer.deserialize(jsonElement, _type, ctx)

      checkNoNullValue(result.getColumn)
      checkNoNullValue(result.getType)
      checkNoNullValue(result.getValue)
    }

    "the json object has no value" in new TestValues {

      val parser = new JsonParser()
      val parsed: JsonObject = parser.parse(COLUMN_NAME_TYPE_VALUE_STRING_EXAMPLE_NO_VALUE_BUT_OK).asInstanceOf[JsonObject]
      when(jsonElement.getAsJsonObject).thenReturn(parsed)

      val result = deserializer.deserialize(jsonElement, _type, ctx)

      checkNoNullValue(result.getColumn)
      checkNoNullValue(result.getType)
      checkNullValue(result.getValue)
    }
  }
}
