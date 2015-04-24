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
package com.stratio.streaming.unit.engine.api

import org.scalatest._
import com.stratio.streaming.utils.StreamsParser
import scala.collection.JavaConversions._
import com.stratio.streaming.commons.constants.{StreamAction, ColumnType}
import com.stratio.streaming.commons.exceptions.StratioAPIGenericException

class StreamsParserUnitTests
  extends FunSpec
  with GivenWhenThen
  with ShouldMatchers {

  describe("The Stream Parser") {
    it("should parse the list of streams from the json") {
      Given("a json response with 2 streams")
      val jsonResponse = """{"count":2,"timestamp":1402495316160,"streams":[{"streamName":"alarms","columns":[{"column":"column1","type":"STRING"}],"queries":[],"activeActions":[],"userDefined":false},{"streamName":"unitTestsStream","columns":[{"column":"column2","type":"BOOLEAN"}],"queries":[{"queryId":"alarms-16aefa02-93e3-4fbd-8e53-1276bdce2c2b","query":"from unitTestsStream select column1 insert into alarms for current-events"}],"activeActions":["SAVE_TO_CASSANDRA"],"userDefined":true}]}"""
      When("we parse the json")
      val streamsList = StreamsParser.parse(jsonResponse)
      Then("we should get the list of streams")
      streamsList.size should be(2)
      And("the first stream should contain all the info")
      val firstStream = streamsList(0)
      firstStream.getStreamName should be("alarms")
      val firstStreamColumns = firstStream.getColumns.toList
      val firstStreamfirstColumn = firstStreamColumns(0)
      firstStreamfirstColumn.getColumn should be ("column1")
      firstStreamfirstColumn.getType should be (ColumnType.STRING)
      val firstStreamQueries = firstStream.getQueries.toList
      firstStreamQueries.size() should be(0)
      val firstStreamActiveActions = firstStream.getActiveActions.toList
      firstStreamActiveActions.size() should be(0)
      firstStream.getUserDefined should be(false)
      And("the second stream should contain all the info")
      val secondStream = streamsList(1)
      secondStream.getStreamName should be("unitTestsStream")
      val secondStreamColumns = secondStream.getColumns.toList
      val secondStreamFirstColumn = secondStreamColumns(0)
      secondStreamFirstColumn.getColumn should be ("column2")
      secondStreamFirstColumn.getType should be (ColumnType.BOOLEAN)
      val secondStreamQueries = secondStream.getQueries.toList
      val secondStreamFirstQuery = secondStreamQueries(0)
      secondStreamFirstQuery.getQuery should be ("from unitTestsStream select column1 insert into alarms for current-events")
      secondStreamFirstQuery.getQueryId should be("alarms-16aefa02-93e3-4fbd-8e53-1276bdce2c2b")
      val secondStreamActiveActions = secondStream.getActiveActions
      secondStreamActiveActions should contain(StreamAction.SAVE_TO_CASSANDRA)
      secondStream.getUserDefined should be(true)
    }

    it("should return an empty list when the list of streams is empty") {
      Given("a json response with 0 streams")
      val jsonResponse = """{"count":0,"timestamp":1402495030220,"streams":[]}"""
      When("we parse the json")
      val streamsList = StreamsParser.parse(jsonResponse)
      Then("we should get an empty list")
      streamsList should be('empty)
    }

    it("should throw a StratioAPIGenericException when it can not parse the json") {
      Given("the json response")
      val jsonResponse = """{"count":3,"timestamp":1396450032105}"""
      When("we parse the json")
      Then("it should throw a StratioAPIGenericException")
      intercept[StratioAPIGenericException] {
        StreamsParser.parse(jsonResponse)
      }
    }
  }
}
