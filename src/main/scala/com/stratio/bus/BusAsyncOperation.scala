package com.stratio.bus

case class BusAsyncOperation(tableProducer: KafkaProducer,
                              operation: String) {
  def performAsyncOperation(queryString: String) = {
    addMessageToKafkaTopic(queryString)
  }

  def addMessageToKafkaTopic(queryString: String) = tableProducer.send(queryString, operation)
}
