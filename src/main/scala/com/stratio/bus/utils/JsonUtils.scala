package com.stratio.bus.utils

import scala.util.parsing.json.JSON._


object JsonUtils {
  def appendElementsToJsonString(json:String,
                    elements: Map[String, Any]) = {
    val parsedJson = parseFull(json)

    val parsedJsonWithElements = parsedJson match {
       case Some(m: Map[String, Any]) => m ++ elements
       case None => Map()
      }

    parsedJsonWithElements.view map {
      case (key, value) => "\""+ key + "\": \"" + value + "\""
    } mkString ("{", ", ", "}")
  }
}
