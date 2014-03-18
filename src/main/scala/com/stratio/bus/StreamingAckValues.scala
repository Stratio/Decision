package com.stratio.bus

object StreamingAckValues extends Enumeration {
  val AckOk = AckValue("0")
  val AckError = AckValue("1")

  def AckValue(name: String): Value with Matching =
    new Val(nextId, name) with Matching

  // enables matching against all Role.Values
  def unapply(s: String): Option[Value] =
    values.find(s == _.toString)

  trait Matching {
    // enables matching against a particular Role.Value
    def unapply(s: String): Boolean =
      (s == toString)
  }
}
