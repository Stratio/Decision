package com.stratio.bus

import java.util.UUID
import com.stratio.streaming.commons.messages.StratioStreamingMessage
import com.stratio.streaming.commons.exceptions.StratioEngineOperationException
import com.stratio.bus.kafka.KafkaProducer
import com.stratio.bus.zookeeper.ZookeeperConsumer
import com.stratio.streaming.commons.constants.REPLY_CODES._

case class StreamingAPISyncOperation(
  kafkaProducer: KafkaProducer,
  zookeeperConsumer: ZookeeperConsumer)
  extends StreamingAPIOperation {

  /**
   * Sends the message to the StratioStreamingEngine and waits
   * for the Acknowledge to be written in zookeeper.
   *
   * @param message
   */
  def performSyncOperation(message: StratioStreamingMessage) = {
    val zNodeUniqueId = UUID.randomUUID().toString
    addMessageToKafkaTopic(message, zNodeUniqueId, kafkaProducer)
    val syncOperationResponse = waitForTheStreamingResponse(zookeeperConsumer, message)
    manageStreamingResponse(syncOperationResponse, message)
  }

  private def manageStreamingResponse(response: String, message: StratioStreamingMessage) = {
    val replyCode = getResponseCode(response)
    replyCode match {
      case Some(OK) => log.info("StratioEngine Ack received for: "+message.getRequest)
      case _ => {
        createLogError(response, message.getRequest)
        throw new StratioEngineOperationException("StratioEngine error: "+getReadableErrorFromCode(replyCode.get))
      }
    }
  }

  private def getResponseCode(response: String): Option[Integer] = {
    try {
      Some(new Integer(response))
    } catch {
      case ex: NumberFormatException => None
    }
  }

  private def createLogError(responseCode: String, queryString: String) = {
    log.error(s"StratioAPI - [ACK_CODE,QUERY_STRING]: [$responseCode,$queryString]")
  }
}
