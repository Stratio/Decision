/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

