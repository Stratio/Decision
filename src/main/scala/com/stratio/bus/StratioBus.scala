package com.stratio.bus

import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent._


class StratioBus
  extends IStratioBus {
    import StratioBus._

  def create(tableName: String, tableValues: Map[String, BusDataTypes.DataType]) = {
    //TODO David
    val groupId = UUID.randomUUID().toString
    //Create topic
    KafkaTopicUtils.createTopic("localhost:2181",tableName,1,1)
    //Create topic consumer
    val createTableConsumer = new KafkaConsumer(tableName, groupId, "localhost:2181", false)

    //Send message to the create table producer topic
    createTableProducer.send(s"table $tableName created")

    //Waiting for the Ack...
    try {
      println(s"Consumer for topic: $tableName is waiting for the response - max 10 seconds...")
      Await.result(createTableConsumer.read(messageCreatedOk), 10 seconds)
    } catch {
      case e: TimeoutException => {
        val producer = new KafkaProducer(tableName, "localhost:9092")
        producer.send("ack")
      }
    }

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
