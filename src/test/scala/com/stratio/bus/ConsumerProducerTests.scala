package com.stratio.bus

import org.scalatest.{ShouldMatchers, FunSpec}
import java.util.UUID


class ConsumerProducerTests
  extends FunSpec
  with ShouldMatchers {
  describe("Simple producer and consumer") {
    it("should send a string to the broker and consume the string back"){
      val testMessage = UUID.randomUUID().toString
      val testTopic = UUID.randomUUID().toString
      val groupId1 = UUID.randomUUID().toString

      var testStatus = false

      println("Starting sample broker testing...")
      val producer = new KafkaProducer(testTopic, "localhost:9092")
      producer.send(testMessage)

      val consumer = new KafkaConsumer(testTopic, groupId1, "localhost:2181" )

      def exec(binaryObject: Array[Byte]) = {
        val message = new String(binaryObject)
        info("testMessage = " + testMessage + " and consumed message = " + message)
        testMessage should be equals(message)
        consumer.close()
        testStatus = true
      }

      info("KafkaSpec is waiting some seconds")
      consumer.read(exec)
      info("KafkaSpec consumed")

      testStatus should be (true)
    }


    it("should send string to broker and consume that string back in different consumer groups") {
      val testMessage = UUID.randomUUID().toString
      val testTopic = UUID.randomUUID().toString
      val groupId_1 = UUID.randomUUID().toString
      val groupId_2 = UUID.randomUUID().toString

      var testStatus1 = false
      var testStatus2 = false

      info("starting sample broker testing")
      val producer = new KafkaProducer(testTopic,"localhost:9092")
      producer.send(testMessage)

      val consumer1 = new KafkaConsumer(testTopic,groupId_1,"localhost:2181")

      def exec1(binaryObject: Array[Byte]) = {
        val message1 = new String(binaryObject)
        println("testMessage 1 = " + testMessage + " and consumed message 1 = " + message1)
        testMessage should be equals(message1)
        consumer1.close()
        testStatus1 = true
      }

      println("KafkaSpec : consumer 1 - is waiting some seconds")
      consumer1.read(exec1)
      println("KafkaSpec : consumer 1 - consumed")

      val consumer2 = new KafkaConsumer(testTopic,groupId_2,"localhost:2181")

      def exec2(binaryObject: Array[Byte]) = {
        val message2 = new String(binaryObject)
        println("testMessage 2 = " + testMessage + " and consumed message 2 = " + message2)
        testMessage should be equals(message2)
        consumer2.close()
        testStatus2 = true
      }

      println("KafkaSpec : consumer 2 - is waiting some seconds")
      consumer2.read(exec2)
      println("KafkaSpec : consumer 2 - consumed")

      testStatus2 should be (true)
    }
  }
}

