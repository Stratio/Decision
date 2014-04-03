package com.stratio.bus

import com.stratio.streaming.commons.streams.StratioStream
import com.stratio.streaming.commons.messages.StratioStreamingMessage
import scala.collection.JavaConversions._
import java.util.UUID
import com.stratio.bus.kafka.KafkaProducer
import com.stratio.bus.zookeeper.ZookeeperConsumer
import com.stratio.bus.utils.StreamsParser
import java.util.List

class StreamingAPIListOperation(kafkaProducer: KafkaProducer,
                             zookeeperConsumer: ZookeeperConsumer)
  extends StreamingAPIOperation {

  def getStreamsList(): List[StratioStream] = {
    val zNodeUniqueId = UUID.randomUUID().toString
    val message = createListRequestMessage()
    addMessageToKafkaTopic(message, zNodeUniqueId, kafkaProducer)
    val jsonStreamingResponse = waitForTheStreamingResponse(zookeeperConsumer, message)
    println(jsonStreamingResponse)
    val parsedList = StreamsParser.parse(jsonStreamingResponse)
    parsedList
  }

  private def createListRequestMessage() = {
    val message = new StratioStreamingMessage()
    message.setOperation("list")
    message.setSession_id(""+System.currentTimeMillis())
    message.setRequest_id(""+System.currentTimeMillis())
    message.setTimestamp(new java.lang.Long(System.currentTimeMillis))
    message
  }
}
