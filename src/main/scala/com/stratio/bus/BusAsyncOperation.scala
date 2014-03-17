package com.stratio.bus

case class BusAsyncOperation(tableProducer: KafkaProducer) {
  def addMessageToKafkaTopic(queryString: String) = tableProducer.send(queryString)

  def performAsyncOperation(queryString: String) = {
    addMessageToKafkaTopic(queryString)
  }
}
