package com.stratio.streaming.api

import com.typesafe.config.ConfigFactory

class StratioStreamingAPIConfig {
  val config = ConfigFactory.load("stratio-streaming.conf")
  val streamingAckTimeOutInSeconds = 4
}
