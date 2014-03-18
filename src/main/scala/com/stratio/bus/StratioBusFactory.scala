package com.stratio.bus

object StratioBusFactory {
  def create(): IStratioBus = {
    new StratioBus()
  }
}
