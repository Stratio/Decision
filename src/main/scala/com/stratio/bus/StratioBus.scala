package com.stratio.bus

class StratioBus
  extends IStratioBus {
    import StratioBus._

  def create = {
    createTableProducer.send("mensaje de prueba")
  }

  def insert = ???

  def select = ???
}

object StratioBus {
  val createTopicName = "stratio_bus_create"

  //TODO host and port -> properties file
  lazy val createTableProducer = new KafkaProducer(createTopicName, "localhost:9092")

  def initializeTopic(topicName: String) {
    //TODO host and port -> properties file
    KafkaTopicUtils.createTopic("localhost:2181",topicName,1,1)
  }
  
  def apply() = {
    initializeTopic(createTopicName)
    new StratioBus()
  }
}
