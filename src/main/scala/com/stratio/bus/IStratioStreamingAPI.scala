package com.stratio.bus

import com.stratio.streaming.commons.messages.StratioStreamingMessage
import com.stratio.streaming.commons.streams.StratioStream

trait IStratioStreamingAPI {
  def initialize(): IStratioStreamingAPI

  def send(message: StratioStreamingMessage)

  def getStreamsList(): List[StratioStream]
}
