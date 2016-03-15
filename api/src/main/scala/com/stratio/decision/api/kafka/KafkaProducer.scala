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

import java.io.{ByteArrayOutputStream, Closeable}
import java.util.{Properties, UUID}

import com.stratio.decision.commons.avro.InsertMessage
import kafka.producer._
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificDatumWriter
import org.slf4j.LoggerFactory

import org.apache.kafka.clients.producer.{ProducerRecord}

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

  val propsAvro = new Properties()
  propsAvro.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
  propsAvro.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.ByteArraySerializer") // Kafka avro message stream comes in as a byte array
  propsAvro.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringSerializer")
  val producerAvro = new org.apache.kafka.clients.producer.KafkaProducer[String, Array[Byte]](propsAvro)

  override def close(): Unit = {
    producer.close
    producerAvro.close
  }

  def send(message: String, key: String) : Unit = {
    send(message, key, null)
  }

  def send(message: String, key: String, anotherTopic:String) : Unit = {

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

  def sendAvro(insertMessage: InsertMessage, key: String) : Unit = {
    sendAvro(insertMessage, key, null)
  }

  def sendAvro(insertMessage: InsertMessage, key: String, anotherTopic:String) : Unit = {
    try {

      val destinationTopic:String = if (anotherTopic!=null) anotherTopic else topic

      log.info("Sending Avro Message")

      val insertBytes = serializeInsertMessageToAvro(insertMessage) // Avro schema serialization as a byte array

      val message = new ProducerRecord[String, Array[Byte]](destinationTopic, key, insertBytes)
      producerAvro.send(message)

    } catch {
      case e: Exception =>
        log.error("Sending Avro Message")
        log.error("Exception: " + e.getMessage)
    }
  }


  def serializeInsertMessageToAvro(insertMessage: InsertMessage): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get.binaryEncoder(out, null)
    val writer = new SpecificDatumWriter[InsertMessage](InsertMessage.getClassSchema)

    writer.write(insertMessage, encoder)
    encoder.flush
    out.close
    out.toByteArray
  }



}