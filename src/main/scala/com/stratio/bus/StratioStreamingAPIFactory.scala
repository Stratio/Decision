package com.stratio.bus

object StratioStreamingAPIFactory {
  def create(): IStratioStreamingAPI = {
    new StratioStreamingAPI()
  }
}
