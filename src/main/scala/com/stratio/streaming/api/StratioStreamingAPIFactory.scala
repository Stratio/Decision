package com.stratio.streaming.api

object StratioStreamingAPIFactory {
  /**
   * Returns a new instance of the StratioStreamingAPI.
   * @return
   */
  def create(): IStratioStreamingAPI = {
    new StratioStreamingAPI()
  }
}
