package com.stratio.bus

import java.util.UUID

class StratioBus
  extends IStratioBus {
    import StratioBus._

  def create(tableName: String, tableValues: Map[String, BusDataTypes.DataType]) = {
    //TODO David?????
    val groupId = UUID.randomUUID().toString
    //Create topic
    KafkaTopicUtils.createTopic("localhost:2181",tableName,1,1)
    //Create topic consumer
    val createTableConsumer = new KafkaConsumer(tableName, groupId, "localhost:2181")

    //Send message to the create table producer topic
    createTableProducer.send(s"table $tableName created")

    //Waiting for the Ack...
    println(s"Consumer for topic: $tableName is waiting for the response")
    createTableConsumer.read(messageCreatedOk)

    def messageCreatedOk(createdMessage: Array[Byte]) = {
      val messageContent = new String(createdMessage)
      println("Message  = " + messageContent + " consumed!!!!")
      createTableConsumer.close()
    }
  }

  def insert = ???

  def select = ???
}

object StratioBus {
  val createTopicName = "stratio_bus_create"

  //TODO host and port -> properties file
  lazy val createTableProducer = new KafkaProducer(createTopicName, "localhost:9092")

  def initializeTopics(topicName: String) {
    //TODO host and port -> properties file
    KafkaTopicUtils.createTopic("localhost:2181",topicName,1,1)
    //TODO create insert and select topics
  }
  
  def apply() = {
    initializeTopics(createTopicName)
    new StratioBus()
  }
}
