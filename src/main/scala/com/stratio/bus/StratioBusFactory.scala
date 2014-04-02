package com.stratio.bus

object StratioBusFactory {
  def create(): IStratioStreamingAPI = {
    new StratioStreamingAPI()
  }
}
