package com.stratio.bus

import com.google.gson.Gson
import com.stratio.streaming.commons.messages.StratioStreamingMessage
import com.stratio.bus.kafka.KafkaProducer

case class BusAsyncOperation(tableProducer: KafkaProducer) {
  def performAsyncOperation(message: StratioStreamingMessage) = {
    addMessageToKafkaTopic(message)
  }

  def addMessageToKafkaTopic(message: StratioStreamingMessage) = {
    val kafkaMessage = new Gson().toJson(message)
    val operation = message.getOperation
    tableProducer.send(kafkaMessage, operation)
  }
}
