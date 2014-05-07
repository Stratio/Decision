package com.stratio.streaming.api

import com.stratio.streaming.commons.streams.StratioStream
import com.stratio.streaming.commons.messages.StratioStreamingMessage
import scala.collection.JavaConversions._
import java.util.UUID
import com.stratio.streaming.kafka.KafkaProducer
import com.stratio.streaming.zookeeper.ZookeeperConsumer
import com.stratio.streaming.utils.StreamsParser
import java.util.List

class StreamingAPIListOperation(kafkaProducer: KafkaProducer,
                             zookeeperConsumer: ZookeeperConsumer)
  extends StreamingAPIOperation {

  def getListStreams(message: StratioStreamingMessage): List[StratioStream] = {
    val zNodeUniqueId = UUID.randomUUID().toString
    addMessageToKafkaTopic(message, zNodeUniqueId, kafkaProducer)
    val jsonStreamingResponse = waitForTheStreamingResponse(zookeeperConsumer, message)
    val parsedList = StreamsParser.parse(jsonStreamingResponse)
    parsedList
  }
}
