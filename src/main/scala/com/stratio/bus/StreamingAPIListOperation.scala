package com.stratio.bus

import com.stratio.streaming.commons.streams.StratioStream
import com.google.gson.Gson
import com.stratio.streaming.commons.messages.{StratioStreamingMessage, ListStreamsMessage}
import scala.collection.JavaConversions._
import java.util.UUID
import com.stratio.bus.kafka.KafkaProducer
import com.stratio.bus.zookeeper.ZookeeperConsumer

class StreamingAPIListOperation(kafkaProducer: KafkaProducer,
                             zookeeperConsumer: ZookeeperConsumer)
  extends StreamingAPIOperation {

  def getStreamsList(): List[StratioStream] = {
    val zNodeUniqueId = UUID.randomUUID().toString
    val message = createListRequestMessage()
    addMessageToKafkaTopic(message, zNodeUniqueId, kafkaProducer)
    val jsonStreamingResponse = waitForTheStreamingResponse(zookeeperConsumer, message)
    parseTheStreamingResponse(jsonStreamingResponse)
  }

  private def createListRequestMessage() = {
    val message = new StratioStreamingMessage()
    message.setOperation("list")
    message.setSession_id(""+System.currentTimeMillis())
    message.setRequest_id(""+System.currentTimeMillis())
    message.setTimestamp(new java.lang.Long(System.currentTimeMillis))
    message
  }

  private def parseTheStreamingResponse(jsonStreamingResponse: String): List[StratioStream] = {
    val listStreams = new Gson().fromJson(jsonStreamingResponse, classOf[ListStreamsMessage]).getStreams.toList
    val stratioStreams = listStreams.map(
      stream => new StratioStream(
        stream.getStreamName,
        stream.getColumns,
        stream.getQueries))
    stratioStreams
  }
}
