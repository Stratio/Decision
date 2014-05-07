package com.stratio.streaming.unit

import org.scalatest.{ShouldMatchers, FunSpec}
import java.util.UUID
import com.stratio.streaming.kafka.{KafkaProducer, KafkaConsumer}


class ConsumerProducerTests
  extends FunSpec
  with ShouldMatchers {
  describe("Simple producer and consumer") {
    ignore("should send a string to the broker and consume the string back"){
      val testMessage = "testMessage"
      val testTopic = UUID.randomUUID().toString
      val messageKey = UUID.randomUUID().toString

      var testStatus = false

      val producer = new KafkaProducer(testTopic, "localhost:9092")
      producer.send(testMessage, messageKey)

      val consumer = new KafkaConsumer(testTopic, "localhost:2181")

      def exec(binaryObject: Array[Byte]) = {
        val message = new String(binaryObject)
        testMessage should be (message)
        consumer.close()
        testStatus = true
      }

      //consumer.read(exec)
      Thread.sleep(2000)
      testStatus should be (true)
    }
  }
}

