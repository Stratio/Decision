package com.stratio.bus

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
  props.put("producer.type", if(synchronously) "sync" else "async")
  props.put("metadata.broker.list", brokerList)
  props.put("batch.num.messages", batchSize.toString)
  props.put("message.send.max.retries", messageSendMaxRetries.toString)
  props.put("require.requred.acks",requestRequiredAcks.toString)
  props.put("client.id",clientId.toString)

  val producer = new Producer[AnyRef, AnyRef](new ProducerConfig(props))

  def send(message: String, key: String) = {
    val messageToBytes = message.getBytes("UTF8")
    val keyToBytes = message.getBytes("UTF8")
    try {
      log.info("Stratio Bus - Sending KeyedMessage[key, value]: ["+key+","+message+"]")
      producer.send(new KeyedMessage(topic, keyToBytes, messageToBytes))
    } catch {
      case e: Exception =>
        log.error("Stratio Bus - Error sending KeyedMessage[key, value]: ["+key+","+message+"]")
    }
  }
}
