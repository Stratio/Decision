package com.stratio.bus

import com.stratio.streaming.commons.messages.StratioStreamingMessage


trait IStratioBus {
  def initialize(): IStratioBus

  def send(message: StratioStreamingMessage)

}
