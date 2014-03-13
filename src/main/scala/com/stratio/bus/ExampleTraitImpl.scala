package com.stratio.bus


class ApiBus extends ApiBusTrait {
  def saluda() = { println("Hellooo") }
}

object ApiBus {
    val connection = {
      println("creating new connection")
      //new TestConnection()
    }
}
