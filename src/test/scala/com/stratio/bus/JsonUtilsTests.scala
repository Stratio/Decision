package com.stratio.bus

import org.scalatest.{GivenWhenThen, FunSpec, ShouldMatchers}
import com.stratio.bus.utils.JsonUtils


class JsonUtilsTests
  extends FunSpec
  with ShouldMatchers
  with GivenWhenThen {

  it("should append the uniqueId to the input json") {
    val command = "create table TABLE_NAME"
    val uniqueId = "1234-6789-3893-9000"
    Given("an input json")
    val inputJson = s"""{"command": "$command"}"""
    When("i append the uniqueid")
    val jsonWithUniqueId = JsonUtils.appendElementsToJson(inputJson, Map("uniqueId" -> uniqueId))
    Then("the json should contain the uniqueid")
    val expectedJson = s"""{"command": "$command", "uniqueId": "$uniqueId"}"""
    expectedJson should be (jsonWithUniqueId)
  }

}
