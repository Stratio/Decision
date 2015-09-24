package com.stratio.streaming.unit.siddhi.query.model

import scala.collection.mutable.ArrayBuffer
import scalaj.collection.Imports._


/**
 * Created by aitor on 9/17/15.
 */

class OrderStream {
  var order_id: String = _
  var timestamp: String = _
  var day_time_zone: String = _
  var client_id: Int = _
  var payment_method: String = _
  var latitude: Double = _
  var longitude: Double = _
  var credit_card: String = _
  var shopping_center: String = _
  var channel: String = _
  var city: String = _
  var country: String = _
  var employee: Int = _
  var total_amount: Float = _
  var total_products: Int = _
  var order_size: String = _
  var lines: String = _

  def getData: Array[Any] = {
    Array[Any](order_id, timestamp, day_time_zone, client_id, payment_method, latitude, longitude,
      credit_card, shopping_center, channel, city, country, employee, total_amount,
      total_products, order_size, lines)
  }

}
object OrderStream {

  def getFromList(m: java.util.List[Array[String]]) : java.util.List[OrderStream] =  {

    //var listOrders= List[OrderStream]()
    var listOrders= ArrayBuffer[OrderStream]()

    //val scalaMap = m.asScala.mapValues(_.toList)
    val scalaMap= m.asScala//.mapValues(_.toList)
    //val scalaMap = scala.collection.convert.decorateAsScala.mapAsScalaMapConverter(m).asScala

    var counter= 0
    scalaMap.foreach { line =>
      //println("# Line: " + line)
      //listOrders :: getOrderStreamFromArray(line.toList)
      listOrders += getOrderStreamFromArray(line.toList)

    }

    listOrders.asJava
    //return scala.collection.convert.decorateAsJava.bufferAsJavaListConverter(listOrders)

  }

  def getOrderStreamFromArray(a: List[String]) : OrderStream =  {
    var orderStream= new OrderStream()
    orderStream.order_id= a(0)
    orderStream.timestamp= a(1)
    orderStream.day_time_zone= a(2)
    orderStream.client_id= a(3).toInt
    orderStream.payment_method= a(4)
    orderStream.latitude= a(5).toDouble
    orderStream.longitude= a(6).toDouble
    orderStream.credit_card= a(7)
    orderStream.shopping_center= a(8)
    orderStream.channel= a(9)
    orderStream.city= a(10)
    orderStream.country= a(11)
    orderStream.employee= a(12).toInt
    orderStream.total_amount= a(13).toFloat
    orderStream.total_products= a(14).toInt
    orderStream.order_size= a(15)
    orderStream.lines= a(16)

    return orderStream
  }
}

