package com.stratio.bus

import org.scalatest.{ShouldMatchers, GivenWhenThen, FunSpec}


class TopicUtilsTests
  extends FunSpec
  with GivenWhenThen
  with ShouldMatchers {

  describe("The topic utils class") {
    it("should create a new topic if it does not exist") {
         Given("")
         When("I create the topic")
         //val topicReturn = KafkaTopicUtils.createTopic("myTopic")
         Then("The topic should have been created")
         //topicReturn should be (true)
    }
  }

}
