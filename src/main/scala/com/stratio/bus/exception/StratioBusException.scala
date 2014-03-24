package com.stratio.bus.exception

class StratioBusException private(ex: RuntimeException) extends RuntimeException(ex) {
  def this(message:String) = this(new RuntimeException(message))
  def this(message:String, throwable: Throwable) = this(new RuntimeException(message, throwable))
}

object StratioBusException {
  def apply(message:String) = new StratioBusException(message)
  def apply(message:String, throwable: Throwable) = new StratioBusException(message, throwable)
}