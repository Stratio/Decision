package com.stratio.bus

import com.stratio.streaming.commons.streams.StratioStream
import com.stratio.streaming.commons.constants.{ReplyCodes, Paths}
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

  def getStreamsList(): List[StratioStream] = {
    val zNodeUniqueId = UUID.randomUUID().toString
    val message = createListRequestMessage()
    addMessageToKafkaTopic(message, zNodeUniqueId, kafkaProducer)
    val jsonStreamingResponse = waitForTheStreamingResponse(message)
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
    val listStreams = new Gson().fromJson(jsonStreamingResponse, classOf[ListStreamsMessage]).getStreams.toList
    val stratioStreams = listStreams.map(
      stream => new StratioStream(
        stream.getStreamName,
        stream.getColumns,
        stream.getQueries))
    stratioStreams
  }
}
