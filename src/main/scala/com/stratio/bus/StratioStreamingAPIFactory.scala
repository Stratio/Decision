package com.stratio.bus

object StratioStreamingAPIFactory {
  /**
   * Returns a new instance of the StratioStreamingAPI.
   * @return
   */
  def create(): IStratioStreamingAPI = {
    new StratioStreamingAPI()
  }
}
