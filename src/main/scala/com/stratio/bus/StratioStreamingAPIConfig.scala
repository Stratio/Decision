package com.stratio.bus

import com.typesafe.config.ConfigFactory
import java.io.File

class StratioStreamingAPIConfig {
  val config = ConfigFactory.load("stratio-streaming.conf")
}
