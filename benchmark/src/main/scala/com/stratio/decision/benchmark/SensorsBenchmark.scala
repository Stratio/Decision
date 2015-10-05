package com.stratio.decision.benchmark

import com.stratio.streaming.api.StratioStreamingAPIFactory
import com.typesafe.config.{Config, ConfigFactory}

object SensorsBenchmark {

  var kafkaHosts= "localhost"
  var zookeeperHosts= "localhost"


  def main(args: Array[String]): Unit = {
    val config: Config = ConfigFactory.load()

    val decisionApi= StratioStreamingAPIFactory.create()
    decisionApi.
  }

}