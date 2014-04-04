package com.stratio.bus

import com.stratio.streaming.commons.streams.StratioStream
import java.util.List
import com.stratio.bus.messaging.{ColumnNameValue, ColumnNameType}
import com.stratio.streaming.commons.exceptions.{StratioAPISecurityException, StratioEngineStatusException, StratioEngineOperationException}

trait IStratioStreamingAPI {
  /**
   * Initializes the StratioStreamingAPI instance.
   * @return
   */
  def initialize(): IStratioStreamingAPI

  /**
   * Creates a new stream.
   * @param streamName
   * @param columns
   */
  @throws(classOf[StratioEngineStatusException])
  @throws(classOf[StratioAPISecurityException])
  @throws(classOf[StratioEngineOperationException])
  def createStream(streamName: String, columns: List[ColumnNameType])

  /**
   * Adds columns to a stream.
   * @param streamName
   * @param columns
   */
  @throws(classOf[StratioEngineStatusException])
  @throws(classOf[StratioAPISecurityException])
  @throws(classOf[StratioEngineOperationException])
  def alterStream(streamName: String, columns: List[ColumnNameType])

  /**
   * Inserts new data into a stream.
   * @param streamName
   * @param data
   */
  @throws(classOf[StratioEngineStatusException])
  @throws(classOf[StratioAPISecurityException])
  def insertData(streamName: String, data: List[ColumnNameValue])

  /**
   * Adds a query to a stream.
   * @param streamName
   * @param query
   */
  @throws(classOf[StratioEngineStatusException])
  @throws(classOf[StratioAPISecurityException])
  @throws(classOf[StratioEngineOperationException])
  def addQuery(streamName: String, query: String)

  /**
   * Removes a query from a stream.
   * @param streamName
   * @param queryId
   */
  @throws(classOf[StratioEngineStatusException])
  @throws(classOf[StratioAPISecurityException])
  @throws(classOf[StratioEngineOperationException])
  def removeQuery(streamName: String, queryId: String)

  /**
   * Removes a stream
   * @param streamName
   */
  @throws(classOf[StratioEngineStatusException])
  @throws(classOf[StratioAPISecurityException])
  @throws(classOf[StratioEngineOperationException])
  def dropStream(streamName: String)

  /**
   * Starts listening to a stream.
   * @param streamName
   */
  @throws(classOf[StratioEngineStatusException])
  @throws(classOf[StratioAPISecurityException])
  @throws(classOf[StratioEngineOperationException])
  def listenStream(streamName: String)

  /**
   * Stops listening to a stream.
   * @param streamName
   */
  @throws(classOf[StratioEngineStatusException])
  @throws(classOf[StratioAPISecurityException])
  @throws(classOf[StratioEngineOperationException])
  def stopListenStream(streamName: String)

  /**
   * Gets a list of all the stream that currently exists.
   * @return a list with the streams
   */
  def listStreams(): List[StratioStream]

  //def saveToCassandra(streamName: String)
}
