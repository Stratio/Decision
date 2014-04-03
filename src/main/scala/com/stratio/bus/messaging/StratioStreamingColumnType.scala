package com.stratio.bus.messaging

object StratioStreamingColumnType extends Enumeration("Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat") {
  type WeekDay = Value
  val Sun, Mon, Tue, Wed, Thu, Fri, Sat = Value
}
