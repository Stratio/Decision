package com.stratio.bus

import com.stratio.streaming.commons.messages.StratioStreamingMessage
import com.stratio.streaming.commons.streams.StratioStream


trait IStratioBus {
  def initialize(): IStratioBus

  def send(message: StratioStreamingMessage)

  def getStreamsList(): List[StratioStream]
}
