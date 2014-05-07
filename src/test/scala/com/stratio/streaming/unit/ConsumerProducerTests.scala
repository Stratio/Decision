/*
 * Copyright 2014 Stratio Big Data, S.L.
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

