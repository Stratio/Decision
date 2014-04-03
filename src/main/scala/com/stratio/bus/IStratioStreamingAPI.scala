package com.stratio.bus

import com.stratio.streaming.commons.messages.StratioStreamingMessage
import com.stratio.streaming.commons.streams.StratioStream
import java.util.List
import com.stratio.bus.messaging.ColumnNameType

trait IStratioStreamingAPI {
  def initialize(): IStratioStreamingAPI

  def createStream(streamName: String, columns: List[ColumnNameType])

  def alterStream(streamName: String, columns: List[ColumnNameType])

  def send(message: StratioStreamingMessage)

  def getStreamsList(): List[StratioStream]
}
