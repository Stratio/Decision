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

import java.io.Closeable
import java.util.{Properties, UUID}

import kafka.producer._
import org.slf4j.LoggerFactory

class KafkaProducer(topic: String,
                    brokerList: String,
                    clientId: String = UUID.randomUUID().toString,
                    synchronously: Boolean = true,
                    compress: Boolean = true,
                    batchSize: Integer = 200,
                    messageSendMaxRetries: Integer = 3,
                    requestRequiredAcks: Integer = -1
                     ) extends Closeable {

  val props = new Properties()
  val log = LoggerFactory.getLogger(getClass)
  //val codec = if(compress) DefaultCompressionCodec.codec else NoCompressionCodec.codec

  //props.put("compression.codec", codec.toString)
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("metadata.broker.list", brokerList)

  val producer = new Producer[AnyRef, AnyRef](new ProducerConfig(props))

  override def close(): Unit = {
    producer.close
  }

  def send(message: String, key: String) = {
    try {
      log.info("Sending KeyedMessage[key, value]: [" + key + "," + message + "]")
      producer.send(new KeyedMessage(topic, key, message))
    } catch {
      case e: Exception =>
        log.error("Error sending KeyedMessage[key, value]: [" + key + "," + message + "]")
        log.error("Exception: " + e.getMessage)
    }
  }

  def send(message: String, key: String, anotherTopic:String) = {

    val destinationTopic:String = if (anotherTopic!=null) anotherTopic else topic
    try {
        log.info("Sending KeyedMessage[key, value]: [" + key + "," + message + "] to the topic: " + destinationTopic)
        producer.send(new KeyedMessage(destinationTopic, key, message))

    } catch {
      case e: Exception =>
        log.error("Error sending KeyedMessage[key, value]: [" + key + "," + message + "]")
        log.error("Exception: " + e.getMessage)
    }
  }
}
