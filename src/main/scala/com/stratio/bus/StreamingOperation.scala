package com.stratio.bus

import com.stratio.streaming.commons.messages.StratioStreamingMessage
import com.google.gson.Gson
import com.stratio.bus.kafka.KafkaProducer

class StreamingOperation {
  def addMessageToKafkaTopic(message: StratioStreamingMessage,
                                     creationUniqueId: String,
                                       tableProducer: KafkaProducer) = {
    val kafkaMessage = new Gson().toJson(message)
    tableProducer.send(kafkaMessage, message.getOperation)
  }
}
