package com.stratio.bus.kafka

import java.util.UUID
import java.util.Properties
import kafka.producer._
import org.slf4j.LoggerFactory

case class KafkaProducer(topic: String,
                          brokerList: String,
                          clientId: String = UUID.randomUUID().toString,
                          synchronously: Boolean = true,
                          compress: Boolean = true,
                          batchSize: Integer = 200,
                          messageSendMaxRetries: Integer = 3,
                          requestRequiredAcks: Integer = -1
                          ) {

  val props = new Properties()
  val log = LoggerFactory.getLogger(getClass)
  //val codec = if(compress) DefaultCompressionCodec.codec else NoCompressionCodec.codec

  //props.put("compression.codec", codec.toString)
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("metadata.broker.list", brokerList)

  val producer = new Producer[AnyRef, AnyRef](new ProducerConfig(props))

  def send(message: String, key: String) = {
    try {
      log.info("Stratio Bus - Sending KeyedMessage[key, value]: ["+key+","+message+"]")
      producer.send(new KeyedMessage(topic, key, message))
    } catch {
      case e: Exception =>
        log.error("Stratio Bus - Error sending KeyedMessage[key, value]: ["+key+","+message+"]")
        log.error("Exception: "+e.getMessage)
    }
  }
}
