package com.stratio.streaming.unit.siddhi.query.model

import scala.collection.mutable.ArrayBuffer
import scalaj.collection.Imports._


/**
 * Created by aitor on 9/17/15.
 */

class LineStream {
  var order: OrderStream = _
  var product: String = _
  var family: String = _
  var quantity: Int = _
  var price: Float = _

  def getData: Array[Any] = {
    Array[Any](order.order_id, order.timestamp, order.day_time_zone, order.client_id, order.payment_method,
      order.latitude, order.longitude, order.credit_card, order.shopping_center, order.channel, order.city,
      order.country, order.employee, order.total_amount, order.total_products, order.order_size,
      product, family, quantity, price)
  }

}
object LineStream {

  def getFromList(m: java.util.List[Array[String]]) : java.util.List[LineStream] =  {

    var listLines= ArrayBuffer[LineStream]()

    val scalaMap= m.asScala

    var counter= 0
    scalaMap.foreach { line =>
      listLines += getLinesStreamFromArray(line.toList)

    }

    listLines.asJava
  }

  def getLinesStreamFromArray(a: List[String]) : LineStream =  {
    var orderStream= new OrderStream()
    var lineStream= new LineStream()

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

    lineStream.order= orderStream
    lineStream.product= a(16)
    lineStream.family= a(17)
    lineStream.quantity= a(18).toInt
    lineStream.price= a(19).toFloat

    return lineStream
  }
}

