package com.stratio.decision.unit.siddhi.query.model

import scala.collection.mutable.ArrayBuffer
import scalaj.collection.Imports._


/**
 * Created by aitor on 9/21/15.
 */

class SandboxStream {
  var name: String = _
  var data: Double = _

  def getData: Array[Any] = {
    Array[Any](name, data)
  }

}
object SandboxStream {

  def getFromList(m: java.util.List[Array[String]]) : java.util.List[SandboxStream] =  {

    var listData= ArrayBuffer[SandboxStream]()

    val scalaMap= m.asScala

    var counter= 0
    scalaMap.foreach { line =>
      listData += getSandboxStreamFromArray(line.toList)

    }

    listData.asJava
  }

  def getSandboxStreamFromArray(a: List[String]) : SandboxStream =  {
    var sandboxStream= new SandboxStream()

    sandboxStream.name= a(0)
    sandboxStream.data= a(1).toDouble

    return sandboxStream
  }
}

