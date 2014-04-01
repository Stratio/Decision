package com.stratio.bus

import com.stratio.streaming.commons.streams.StratioStream
import com.stratio.bus.kafka.{KafkaProducer, KafkaConsumer}
import com.stratio.streaming.commons.constants.BUS
import com.google.gson.Gson
import com.stratio.streaming.commons.messages.{StratioStreamingMessage, ListStreamsMessage}
import scala.collection.JavaConversions._
import java.util.UUID
import scala.concurrent._
import com.stratio.bus.kafka.KafkaProducer
import com.stratio.streaming.commons.exceptions.StratioEngineOperationException
import scala.concurrent.duration._

class StreamingStatusOperation(kafkaProducer: KafkaProducer,
                               zookeeperCluster: String)
  extends StreamingOperation {
  var listOfStreams = List[StratioStream]()

  def getStreamingStatus(): List[StratioStream] = {
    val zNodeUniqueId = UUID.randomUUID().toString
    //TODO REFACTOR ME!!!!!!
    val message = new StratioStreamingMessage()
    message.setOperation("list")
    message.setSession_id(""+System.currentTimeMillis())
    message.setRequest_id(""+System.currentTimeMillis())
    message.setTimestamp(new java.lang.Long(System.currentTimeMillis))
    addMessageToKafkaTopic(message, zNodeUniqueId, kafkaProducer)
    val consumer = new KafkaConsumer(BUS.LIST_STREAMS_TOPIC, zookeeperCluster)
    waitForTheStreamingResponse(consumer)
    consumer.close()
    listOfStreams
  }

  def convertListStreamsMessageToStratioStream(binaryObject: Array[Byte]) = {
    println("GET STREAMING STATUS!!!!!")
    val message = new String(binaryObject)
    val listStreamsMessage = new Gson().fromJson(message, classOf[ListStreamsMessage]).getStreams.toList
    println("NUMERO DE STREAMS: "+listStreamsMessage.size)
    listStreamsMessage.foreach(stream => listOfStreams ::= new StratioStream(stream.getStreamName, stream.getColumns))
  }

  private def waitForTheStreamingResponse(consumer: KafkaConsumer) = {
    try {
      Await.result(consumer.read(convertListStreamsMessageToStratioStream), 2 seconds)
    } catch {
      case e: TimeoutException => {
        //throw new StratioEngineOperationException("Acknowledge timeout expired")
      }
    }
  }
}
