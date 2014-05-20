/*
 * Copyright 2014 Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stratio.streaming.api

import _root_.kafka.consumer.KafkaStream
import com.stratio.streaming.commons.streams.StratioStream
import java.util.List
import com.stratio.streaming.messaging.{ColumnNameValue, ColumnNameType}
import com.stratio.streaming.commons.exceptions.{StratioAPISecurityException, StratioEngineStatusException, StratioEngineOperationException}
import com.stratio.streaming.dto.StratioQueryStream
import com.stratio.streaming.commons.messages.{StratioStreamingMessage, ColumnNameTypeValue}

trait IStratioStreamingAPI {
  /**
   * Initializes the StratioStreamingAPI instance.
   * @return
   */
  def initialize(): IStratioStreamingAPI

  /**
   * /**
   * Initializes the StratioStreamingAPI instance.
   * @return
   */
   * @param kafkaServer
   * @param kafkaPort
   * @param theZookeeperServer
   * @param theZookeeperPort
   * @return
   */
  def initializeWithServerConfig(kafkaServer: String,
                 kafkaPort: String,
                 theZookeeperServer: String,
                 theZookeeperPort: String): IStratioStreamingAPI

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
   * @return the query Id
   */
  @throws(classOf[StratioEngineStatusException])
  @throws(classOf[StratioAPISecurityException])
  @throws(classOf[StratioEngineOperationException])
  def addQuery(streamName: String, query: String): String

  /**
   * Removes a query from a stream.
   * @param streamName
   * @param queryId
   */
  @throws(classOf[StratioEngineStatusException])
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
  def listenStream(streamName: String): KafkaStream[String, StratioStreamingMessage]

  /**
   * Stops listening to a stream.
   * @param streamName
   */
  @throws(classOf[StratioEngineStatusException])
  @throws(classOf[StratioAPISecurityException])
  def stopListenStream(streamName: String)



  /**
   * Gets a list of the columns from a given stream.
   * @param stream
   * @return a list with the columns from the given stream
   */
  def columnsFromStream(stream: String): List[ColumnNameTypeValue]

  /**
   * Gets a list of the queries from a given stream.
   * @param stream
   * @return a list with the queries from the given stream
   */
  def queriesFromStream(stream: String): List[StratioQueryStream]

  /**
   * Gets a list of all the stream that currently exists.
   * @return a list with the streams
   */
  @throws(classOf[StratioEngineStatusException])
  def listStreams(): List[StratioStream]

  /**
   * Gets a list of all the stream that currently exists.
   * @return a list with the streams
   */
  @throws(classOf[StratioEngineStatusException])
  @throws(classOf[StratioAPISecurityException])
  def indexStream(stream: String)

  //def saveToCassandra(streamName: String)
}
