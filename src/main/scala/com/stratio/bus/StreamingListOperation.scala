package com.stratio.bus

import com.stratio.streaming.commons.streams.StratioStream
import com.stratio.bus.kafka.KafkaConsumer
import com.stratio.streaming.commons.constants.{ReplyCodes, Paths, BUS}
import com.google.gson.Gson
import com.stratio.streaming.commons.messages.{StratioStreamingMessage, ListStreamsMessage}
import scala.collection.JavaConversions._
import java.util.UUID
import scala.concurrent._
import com.stratio.bus.kafka.KafkaProducer
import scala.concurrent.duration._
import com.stratio.streaming.commons.exceptions.StratioEngineOperationException
import org.slf4j.LoggerFactory
import com.stratio.bus.zookeeper.ZookeeperConsumer
import com.typesafe.config.ConfigFactory

class StreamingListOperation(kafkaProducer: KafkaProducer,
                             zookeeperConsumer: ZookeeperConsumer)
  extends StreamingOperation {
  val log = LoggerFactory.getLogger(getClass)
  val config = ConfigFactory.load()
  val streamingAckTimeOut = config.getString("streaming.ack.timeout.in.seconds").toInt
  val ackErrorList = Map(ReplyCodes.KO_GENERAL_ERROR -> "Generic error",
    ReplyCodes.KO_PARSER_ERROR -> "Parser error",
    ReplyCodes.KO_LISTENER_ALREADY_EXISTS -> "Listener already exists",
    ReplyCodes.KO_QUERY_ALREADY_EXISTS -> "Query already exists",
    ReplyCodes.KO_STREAM_ALREADY_EXISTS -> "Stream already exists",
    ReplyCodes.KO_STREAM_DOES_NOT_EXIST -> "Stream does not exist",
    ReplyCodes.KO_COLUMN_ALREADY_EXISTS -> "Column already exists",
    ReplyCodes.KO_COLUMN_DOES_NOT_EXISTS -> "Column does not exist"
  )

  def getStreamsList(): List[StratioStream] = {
    val zNodeUniqueId = UUID.randomUUID().toString
    val message = createListRequestMessage()
    addMessageToKafkaTopic(message, zNodeUniqueId, kafkaProducer)
    val jsonStreamingResponse = waitForTheStreamingResponse(message)
    parseTheStreamingResponse(jsonStreamingResponse)
  }

  /*
  def convertListStreamsMessageToStratioStream(binaryObject: Array[Byte]) = {
    val message = new String(binaryObject)
    val listStreamsMessage = new Gson().fromJson(message, classOf[ListStreamsMessage]).getStreams.toList
    listOfStreams.drop(listOfStreams.size)
    listStreamsMessage.foreach(stream => listOfStreams ::= new StratioStream(stream.getStreamName, stream.getColumns))
  }*/
  
  private def createListRequestMessage() = {
    val message = new StratioStreamingMessage()
    message.setOperation("list")
    message.setSession_id(""+System.currentTimeMillis())
    message.setRequest_id(""+System.currentTimeMillis())
    message.setTimestamp(new java.lang.Long(System.currentTimeMillis))
    message
  }

  private def waitForTheStreamingResponse(message: StratioStreamingMessage) = {
    val zNodeFullPath = getOperationZNodeFullPath(message)
    try {
      Await.result(zookeeperConsumer.readZNode(zNodeFullPath), streamingAckTimeOut seconds)
      val response = zookeeperConsumer.getZNodeData(zNodeFullPath)
      zookeeperConsumer.removeZNode(zNodeFullPath)
      response.get
    } catch {
      case e: TimeoutException => {
        log.error("StratioAPI - Ack timeout expired for: "+message.getRequest)
        throw new StratioEngineOperationException("Acknowledge timeout expired"+message.getRequest)
      }
    }
  }

  private def getOperationZNodeFullPath(message: StratioStreamingMessage) = {
    val zookeeperBasePath = Paths.ZK_BASE_PATH
    val operation = message.getOperation.toLowerCase()
    val uniqueId = message.getRequest_id
    val zookeeperPath = s"$zookeeperBasePath/$operation/$uniqueId"
    log.info(s"StratioAPI - Waiting for zookeeper node response. Listen to the following path: $zookeeperPath")
    zookeeperPath
  }

  private def parseTheStreamingResponse(jsonStreamingResponse: String): List[StratioStream] = {
     List()
  }
}
