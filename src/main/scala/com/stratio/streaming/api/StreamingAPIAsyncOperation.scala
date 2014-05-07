package com.stratio.streaming.api

import com.google.gson.Gson
import com.stratio.streaming.commons.messages.StratioStreamingMessage
import com.stratio.streaming.kafka.KafkaProducer

case class StreamingAPIAsyncOperation(tableProducer: KafkaProducer) {
  def performAsyncOperation(message: StratioStreamingMessage) = {
    addMessageToKafkaTopic(message)
  }

  def addMessageToKafkaTopic(message: StratioStreamingMessage) = {
    val kafkaMessage = new Gson().toJson(message)
    val operation = message.getOperation
    tableProducer.send(kafkaMessage, operation)
  }
}
