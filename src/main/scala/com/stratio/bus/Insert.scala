package com.stratio.bus

case class Insert(insertTableProducer: KafkaProducer) {
  def createMessageInTheInsertTopic(queryString: String) = insertTableProducer.send(queryString)

  def insert(queryString: String) = {
    createMessageInTheInsertTopic(queryString)
  }
}
