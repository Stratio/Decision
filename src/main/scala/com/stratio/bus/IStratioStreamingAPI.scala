package com.stratio.bus

import com.stratio.streaming.commons.streams.StratioStream
import java.util.List
import com.stratio.bus.messaging.{ColumnNameValue, ColumnNameType}

trait IStratioStreamingAPI {
  def initialize(): IStratioStreamingAPI

  def createStream(streamName: String, columns: List[ColumnNameType])

  def alterStream(streamName: String, columns: List[ColumnNameType])

  def insertData(streamName: String, data: List[ColumnNameValue])

  def addQuery(streamName: String, query: String)

  def removeQuery(streamName: String, queryId: String)

  def dropStream(streamName: String)

  def listenStream(streamName: String)

  def stopListenStream(streamName: String)

  def listStreams(): List[StratioStream]

  //def saveToCassandra(streamName: String)
}
