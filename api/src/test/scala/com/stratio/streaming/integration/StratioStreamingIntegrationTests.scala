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
package com.stratio.streaming.integration

import org.scalatest._
import com.stratio.streaming.commons.exceptions.{StratioEngineOperationException, StratioStreamingException, StratioEngineStatusException, StratioAPISecurityException}
import org.apache.curator.retry.{RetryOneTime, ExponentialBackoffRetry}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import com.stratio.streaming.commons.constants.STREAMING._
import com.stratio.streaming.commons.constants.STREAMING.STATS_NAMES._
import com.stratio.streaming.messaging.{ColumnNameValue, ColumnNameType}
import scala.collection.JavaConversions._
import scala.util.control.Breaks._
import com.stratio.streaming.api.{StratioStreamingAPIConfig, StratioStreamingAPIFactory}
import com.stratio.streaming.zookeeper.ZookeeperConsumer
import com.stratio.streaming.commons.constants._
import com.datastax.driver.core.{Session, Cluster}
import com.datastax.driver.core.querybuilder.QueryBuilder
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.client.methods.{HttpGet, HttpDelete}
import org.apache.http.util.EntityUtils
import com.mongodb._
import com.stratio.streaming.zookeeper.ZookeeperConsumer


class StratioStreamingIntegrationTests
  extends  FunSpec
  with StratioStreamingAPIConfig
  with ShouldMatchers
  with BeforeAndAfterEach
  with BeforeAndAfterAll {

  var zookeeperCluster: String = _
  val retryPolicy = new RetryOneTime(500)
  var zookeeperClient: CuratorFramework = _
  var zookeeperConsumer: ZookeeperConsumer = _
  var streamingAPI = StratioStreamingAPIFactory.create()
  val testStreamName = "unitTestsStream"
  val internalTestStreamName = "stratio_"
  val elasticSearchIndex = "stratiostreaming"
  var elasticSearchHost = ""
  var elasticSearchPort = ""
  var cassandraHost = ""
  var mongoHost = ""
  val internalStreamName = STREAMING.STATS_NAMES.STATS_STREAMS(0)
  var mongoClient:MongoClient = _
  var mongoDataBase:DB = _
  var cassandraCluster: Cluster = _
  var cassandraSession: Session = _

  override def beforeAll(conf: ConfigMap) {
    if (configurationHasBeenDefinedThroughCommandLine(conf)) {
      val zookeeperHost = conf.get("zookeeperHost").get.toString
      val zookeeperPort = conf.get("zookeeperPort").get.toString
      val kafkaHost = conf.get("kafkaHost").get.toString
      val kafkaPort = conf.get("kafkaPort").get.toString
      elasticSearchHost = conf.get("elasticSearchHost").get.toString
      elasticSearchPort = conf.get("elasticSearchPort").get.toString
      streamingAPI.initializeWithServerConfig(kafkaHost,
        kafkaPort.toInt,
        zookeeperHost,
        zookeeperPort.toInt)
      zookeeperCluster = s"$zookeeperHost:$zookeeperPort"
      zookeeperClient = CuratorFrameworkFactory.newClient(zookeeperCluster, retryPolicy)
      zookeeperConsumer = new ZookeeperConsumer(zookeeperClient)
      cassandraHost = conf.get("cassandraHost").get.toString
      mongoHost = conf.get("mongoHost").get.toString
    } else {
      //Pickup the config from stratio-streaming.conf
      zookeeperCluster = config.getString("zookeeper.server")+":"+config.getString("zookeeper.port")
      zookeeperClient = CuratorFrameworkFactory.newClient(zookeeperCluster, retryPolicy)
      zookeeperConsumer = new ZookeeperConsumer(zookeeperClient)
      elasticSearchHost = config.getString("elasticsearch.server")
      elasticSearchPort = config.getString("elasticsearch.port")
      cassandraHost = config.getString("cassandra.host")
      mongoHost = config.getString("mongo.host")
      streamingAPI.initialize()
    }
    zookeeperClient.start()
    cassandraCluster = Cluster.builder()
      .addContactPoint(cassandraHost)
      .build()
    cassandraSession = cassandraCluster.connect()
    mongoClient = new MongoClient(mongoHost)
    mongoDataBase = mongoClient.getDB(STREAMING_KEYSPACE_NAME)
    checkStatusAndCleanTheEngine()
  }

  override def afterAll() {
    cassandraSession.close()
    mongoClient.close()
  }

  def configurationHasBeenDefinedThroughCommandLine(conf: ConfigMap) = {
    conf.get("zookeeperHost").isDefined &&
    conf.get("zookeeperPort").isDefined &&
    conf.get("kafkaHost").isDefined &&
    conf.get("kafkaPort").isDefined &&
    conf.get("elasticSearchHost").isDefined &&
    conf.get("elasticSearchPort").isDefined &&
    conf.get("cassandraHost").isDefined &&
    conf.get("mongoHost").isDefined
  }


  override def beforeEach() {
    checkStatusAndCleanTheEngine()
  }

  def checkStatusAndCleanTheEngine() {
    if (!zookeeperConsumer.zNodeExists(ZK_EPHEMERAL_NODE_PATH)) {
      zookeeperClient.create().forPath(ZK_EPHEMERAL_NODE_PATH)
      //Delay to get rid of flakiness
      Thread.sleep(2000)
    }
    cleanStratioStreamingEngine()
  }

  describe("The create operation") {
    it("should create a new stream when the stream does not exist") {
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
      val secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING)
      val thirdStreamColumn = new ColumnNameType("column3", ColumnType.BOOLEAN)
      val fourthStreamColumn = new ColumnNameType("column4", ColumnType.DOUBLE)
      val fifthStreamColumn = new ColumnNameType("column5", ColumnType.FLOAT)
      val sixthStreamColumn = new ColumnNameType("column6", ColumnType.LONG)
      val columnList = Seq(firstStreamColumn,
        secondStreamColumn,
        thirdStreamColumn,
        fourthStreamColumn,
        fifthStreamColumn,
        sixthStreamColumn)
      try {
        streamingAPI.createStream(testStreamName, columnList)
      } catch {
        case ssEx: StratioStreamingException => fail()
      }

      theNumberOfUserDefinedStreams should be(1)
      theNumberOfColumnsOfTheStream(testStreamName) should be(6)
    }

    it("should throw a StratioEngineOperationException when creating a stream that already exists") {
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
      val secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING)
      val columnList = Seq(firstStreamColumn, secondStreamColumn)
      intercept [StratioEngineOperationException] {
        streamingAPI.createStream(testStreamName, columnList)
        streamingAPI.createStream(testStreamName, columnList)
      }
    }

    it("should throw a StratioAPISecurityException when creating an internal stream") {
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
      val secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING)
      val columnList = Seq(firstStreamColumn, secondStreamColumn)
      intercept [StratioAPISecurityException] {
        streamingAPI.createStream(internalStreamName, columnList)
      }
    }
  }

  describe("The alter operation") {
    it("should add new columns to an existing stream") {
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
      val secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING)
      val columnList = Seq(firstStreamColumn, secondStreamColumn)
      val thirdStreamColumn = new ColumnNameType("column3", ColumnType.INTEGER)
      val newColumnList = Seq(thirdStreamColumn)
      try {
        streamingAPI.createStream(testStreamName, columnList)
        streamingAPI.alterStream(testStreamName, newColumnList)
      } catch {
        case ssEx: StratioStreamingException => fail()
      }

      theNumberOfUserDefinedStreams should be(1)
      theNumberOfColumnsOfTheStream(testStreamName) should be(3)
    }

    it("should throw a StratioEngineOperationException when adding a column that already exists") {
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
      val secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING)
      val columnList = Seq(firstStreamColumn, secondStreamColumn)
      val thirdStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
      val newColumnList = Seq(thirdStreamColumn)
      streamingAPI.createStream(testStreamName, columnList)
      intercept [StratioEngineOperationException] {
        streamingAPI.alterStream(testStreamName, newColumnList)
      }
    }

    it("should throw a StratioEngineOperationException when adding a column to a non existing stream") {
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
      val secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING)
      val columnList = Seq(firstStreamColumn, secondStreamColumn)
      intercept [StratioEngineOperationException] {
        streamingAPI.alterStream(testStreamName, columnList)
      }
    }

    it("should throw a StratioAPISecurityException when adding a column to an internal stream") {
      val thirdStreamColumn = new ColumnNameType("newColumn", ColumnType.INTEGER)
      val newColumnList = Seq(thirdStreamColumn)
      val internalStream = STREAMING.STATS_NAMES.STATS_STREAMS(0)
      intercept [StratioAPISecurityException] {
        streamingAPI.alterStream(internalStream, newColumnList)
      }
    }
  }

  describe("The add query operation") {
    it("should add new queries to an existing stream") {
      val alarmsStream = "alarms"
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
      val secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING)
      val thirdStreamColumn = new ColumnNameType("column3", ColumnType.BOOLEAN)
      val fourthStreamColumn = new ColumnNameType("column4", ColumnType.DOUBLE)
      val fifthStreamColumn = new ColumnNameType("column5", ColumnType.FLOAT)
      val sixthStreamColumn = new ColumnNameType("column6", ColumnType.LONG)
      val columnList = Seq(firstStreamColumn,
        secondStreamColumn,
        thirdStreamColumn,
        fourthStreamColumn,
        fifthStreamColumn,
        sixthStreamColumn)
      val theFirstQuery = s"from $testStreamName select column1, column2, column3, column4, column5, column6 insert into $alarmsStream for current-events"
      try {
        streamingAPI.createStream(alarmsStream, columnList)
        streamingAPI.createStream(testStreamName, columnList)
        streamingAPI.addQuery(testStreamName, theFirstQuery)
      } catch {
        case ssEx: StratioStreamingException => fail()
      }
      Thread.sleep(2000)
      theNumberOfQueriesOfTheStream(testStreamName) should be(1)
    }

    it("should throw a StratioEngineOperationException when creating an existing query") {
      val alarmsStream = "alarms2"
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
      val secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING)
      val thirdStreamColumn = new ColumnNameType("column3", ColumnType.BOOLEAN)
      val fourthStreamColumn = new ColumnNameType("column4", ColumnType.DOUBLE)
      val fifthStreamColumn = new ColumnNameType("column5", ColumnType.FLOAT)
      val sixthStreamColumn = new ColumnNameType("column6", ColumnType.LONG)
      val columnList = Seq(firstStreamColumn,
        secondStreamColumn,
        thirdStreamColumn,
        fourthStreamColumn,
        fifthStreamColumn,
        sixthStreamColumn)
      val theFirstQuery = s"from $testStreamName select column1, column2, column3, column4, column5, column6 insert into $alarmsStream for current-events"
      try {
        streamingAPI.createStream(alarmsStream, columnList)
        streamingAPI.createStream(testStreamName, columnList)
        streamingAPI.addQuery(testStreamName, theFirstQuery)
        Thread.sleep(2000)
        intercept[StratioEngineOperationException] {
          streamingAPI.addQuery(testStreamName, theFirstQuery)
        }
      } catch {
        case ssEx: StratioStreamingException => fail()
      }
    }

    it("should throw a StratioEngineOperationException when creating a wrong query") {
      val alarmsStream = "alarms"
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
      val secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING)
      val columnList = Seq(firstStreamColumn, secondStreamColumn)
      val theQuery = s"from $testStreamName select column1, column2, column3"
      streamingAPI.createStream(alarmsStream, columnList)
      streamingAPI.createStream(testStreamName, columnList)
      intercept [StratioEngineOperationException] {
          streamingAPI.addQuery(testStreamName, theQuery)
      }
    }

    it("should throw a StratioAPISecurityException when adding a query to an internal stream") {
      val theQuery = s"from $testStreamName select column1, column2 insert into $internalStreamName for current-events"
      intercept [StratioAPISecurityException] {
        streamingAPI.addQuery(internalStreamName, theQuery)
      }
    }
  }

  describe("The remove query operation") {
    it("should remove the queries from an existing stream") {
      val alarmsStream = "alarms"
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
      val secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING)
      val columnList = Seq(firstStreamColumn, secondStreamColumn)
      val theQuery = s"from $testStreamName select column1, column2 insert into $alarmsStream for current-events"
      try {
        streamingAPI.createStream(alarmsStream, columnList)
        streamingAPI.createStream(testStreamName, columnList)
        val queryId = streamingAPI.addQuery(testStreamName, theQuery)
        streamingAPI.removeQuery(testStreamName, queryId)
      } catch {
        case ssEx: StratioStreamingException => fail()
      }

      theNumberOfQueriesOfTheStream(testStreamName) should be(0)
    }

    it("should throw a StratioEngineOperationException when removing a non existing query") {
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
      val secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING)
      val columnList = Seq(firstStreamColumn, secondStreamColumn)
      val nonExistingQueryId = "1234"
      intercept [StratioEngineOperationException] {
        streamingAPI.createStream(testStreamName, columnList)
        streamingAPI.removeQuery(testStreamName, nonExistingQueryId)
      }
    }
  }

  describe("The drop operation") {
    it("should remove an existing stream") {
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
      val secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING)

      val columnList = Seq(firstStreamColumn, secondStreamColumn)
      try {
        streamingAPI.createStream(testStreamName, columnList)
        streamingAPI.dropStream(testStreamName)
      } catch {
        case ssEx: StratioStreamingException => fail()
      }
      theNumberOfUserDefinedStreams should be(0)
    }

    it("should throw a StratioEngineOperationException when removing a stream that does not exist") {
      val nonExistingStream = "nonExistingStream"
      intercept [StratioEngineOperationException] {
        streamingAPI.dropStream(nonExistingStream)
      }
    }

    it("should throw a StratioAPISecurityException when removing an internal stream") {
      intercept [StratioAPISecurityException] {
        streamingAPI.dropStream(STREAMING.STATS_NAMES.STATS_STREAMS(1))
      }
    }
  }

  describe("The listen operation") {
    it("should return the stream flow") {
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
      val secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING)
      val columnList = Seq(firstStreamColumn, secondStreamColumn)
      val firstColumnValue = new ColumnNameValue("column1", new Integer(1))
      val secondColumnValue = new ColumnNameValue("column2", "testValue")
      val streamData = Seq(firstColumnValue, secondColumnValue)
      try {
        streamingAPI.createStream(testStreamName, columnList)
        val streams = streamingAPI.listenStream(testStreamName)
        Thread.sleep(2000)
        streamingAPI.insertData(testStreamName, streamData)
        for (stream <- streams) {
          val firstColumn = stream.message.getColumns.get(0)
          firstColumn.getColumn should be("column1")
          firstColumn.getValue should be(1)
          firstColumn.getType should be("INT")
          val secondColumn = stream.message.getColumns.get(1)
          secondColumn.getColumn should be("column2")
          secondColumn.getValue should be("testValue")
          secondColumn.getType should be("STRING")
          break
        }
      } catch {
        case ssEx: StratioStreamingException => fail()
        case _ => assert(true)
      } finally {
        streamingAPI.stopListenStream(testStreamName)
      }
    }

    it("should throw a StratioEngineOperationException when the listener already exists") {
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
      val secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING)
      val columnList = Seq(firstStreamColumn, secondStreamColumn)
      val firstColumnValue = new ColumnNameValue("column1", new Integer(1))
      val secondColumnValue = new ColumnNameValue("column2", "testValue")
      val streamData = Seq(firstColumnValue, secondColumnValue)
      try {
        streamingAPI.createStream(testStreamName, columnList)
        streamingAPI.listenStream(testStreamName)
        Thread.sleep(2000)
        streamingAPI.insertData(testStreamName, streamData)
        Thread.sleep(2000)
        intercept [StratioEngineOperationException] {
          streamingAPI.listenStream(testStreamName)
        }
      } catch {
        case ssEx: StratioStreamingException => fail()
      } finally {
        streamingAPI.stopListenStream(testStreamName)
      }
    }
  }

  describe("The stop listen operation") {
    it("should stop the stream flow") {
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
      val secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING)
      val columnList = Seq(firstStreamColumn, secondStreamColumn)
      val firstColumnValue = new ColumnNameValue("column1", new Integer(1))
      val secondColumnValue = new ColumnNameValue("column2", "testValue")
      val streamData = Seq(firstColumnValue, secondColumnValue)
      streamingAPI.createStream(testStreamName, columnList)
      val streams = streamingAPI.listenStream(testStreamName)
      Thread.sleep(2000)
      streamingAPI.insertData(testStreamName, streamData)
      streamingAPI.stopListenStream(testStreamName)
      for (stream <- streams) {
        fail()
      }
      assert(true)
    }
  }

  describe("The index operation") {
    it("should index the stream to elasticsearch and stop indexing", Tag("index")) {
      cleanElasticSearchIndexes()
      val indexedStreamName = "testindexedstream1"
      val indexedData1 = "testValue1"
      val indexedData2 = "testValue2"
      val indexedData3 = "testValue3"
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.STRING)
      val columnList = Seq(firstStreamColumn)
      val columnDataIndexed1 = new ColumnNameValue("column1", indexedData1)
      val columnDataIndexed2 = new ColumnNameValue("column1", indexedData2)
      val columnDataNotIndexed = new ColumnNameValue("column1", indexedData3)
      val streamDataIndexed1 = Seq(columnDataIndexed1)
      val streamDataIndexed2 = Seq(columnDataIndexed2)
      val streamDataNotIndexed = Seq(columnDataNotIndexed)
      try {
        streamingAPI.createStream(indexedStreamName, columnList)
        streamingAPI.indexStream(indexedStreamName)
        streamingAPI.insertData(indexedStreamName, streamDataIndexed1)
        Thread.sleep(10000)
        theStreamContainsTheData(indexedData1) should be(true)
        streamingAPI.insertData(indexedStreamName, streamDataIndexed2)
        streamingAPI.stopIndexStream(indexedStreamName)
        streamingAPI.insertData(indexedStreamName, streamDataNotIndexed)
        Thread.sleep(10000)
        theStreamContainsTheData(indexedData2) should be(true)
        theStreamContainsTheData(indexedData3) should be(false)
      } catch {
        case ssEx: StratioStreamingException => fail()
      }
    }

    it("should throw a StratioEngineOperationException when the index operation has been already defined", Tag("index")) {
      cleanElasticSearchIndexes()
      val indexedStreamName = "testindexedstream2"
      val indexedData1 = "testValue1"
      val indexedData2 = "testValue2"
      val indexedData3 = "testValue3"
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.STRING)
      val columnList = Seq(firstStreamColumn)
      val columnDataIndexed1 = new ColumnNameValue("column1", indexedData1)
      val columnDataIndexed2 = new ColumnNameValue("column1", indexedData2)
      val columnDataNotIndexed = new ColumnNameValue("column1", indexedData3)
      val streamDataIndexed1 = Seq(columnDataIndexed1)
      try {
        streamingAPI.createStream(indexedStreamName, columnList)
        streamingAPI.indexStream(indexedStreamName)
        streamingAPI.insertData(indexedStreamName, streamDataIndexed1)
        Thread.sleep(10000)
        intercept[StratioEngineOperationException] {
          streamingAPI.indexStream(indexedStreamName)
        }
      } catch {
        case ssEx: StratioStreamingException => fail()
      }
    }
  }

  describe("The save to cassandra operation") {
    it("should add a row to cassandra", Tag("cassandra")) {
      val cassandraStreamName = "cassandrastreamtabletest1"
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.STRING)
      val secondStreamColumn = new ColumnNameType("column2", ColumnType.BOOLEAN)
      val thirdStreamColumn = new ColumnNameType("column3", ColumnType.FLOAT)
      val fourthStreamColumn = new ColumnNameType("column4", ColumnType.INTEGER)
      val fifthStreamColumn = new ColumnNameType("column5", ColumnType.DOUBLE)
      val sixthStreamColumn = new ColumnNameType("column6", ColumnType.LONG)
      val columnList = Seq(firstStreamColumn,
        secondStreamColumn,
        thirdStreamColumn,
        fourthStreamColumn,
        fifthStreamColumn,
        sixthStreamColumn)
      val firstColumnValue = new ColumnNameValue("column1", "testValue")
      val secondColumnValue = new ColumnNameValue("column2", new java.lang.Boolean(true))
      val thirdColumnValue = new ColumnNameValue("column3", new java.lang.Float(2.0))
      val fourthColumnValue = new ColumnNameValue("column4", new java.lang.Integer(4))
      val fifthColumnValue = new ColumnNameValue("column5", new java.lang.Double(5))
      val sixthColumnValue = new ColumnNameValue("column6", new java.lang.Long(600000))
      val streamData = Seq(firstColumnValue,
        secondColumnValue,
        thirdColumnValue,
        fourthColumnValue,
        fifthColumnValue,
        sixthColumnValue)
      try {
        streamingAPI.createStream(cassandraStreamName, columnList)
        streamingAPI.saveToCassandra(cassandraStreamName)
        streamingAPI.insertData(cassandraStreamName, streamData)
        Thread.sleep(2000)
      } catch {
        case ssEx: StratioStreamingException => fail()
      }
      val storedRows = fetchStoredRowsFromCassandra(cassandraStreamName)
      cleanCassandraTable(cassandraStreamName)
      storedRows.size() should be(1)
      val storedRow = storedRows.get(0)
      storedRow.getString("column1") should be("testValue")
      storedRow.getBool("column2") should be(java.lang.Boolean.TRUE)
      storedRow.getFloat("column3") should be(2.0)
      storedRow.getInt("column4") should be(4)
      storedRow.getDouble("column5") should be(5)
      storedRow.getDouble("column6") should be(600000)
    }

    it("should stop adding rows to cassandra", Tag("cassandra")) {
      val cassandraStreamName = "cassandrastreamtabletest2"
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.STRING)
      val columnList = Seq(firstStreamColumn)
      val firstColumnValue = new ColumnNameValue("column1", "testValue")
      val streamData = Seq(firstColumnValue)
      try {
        streamingAPI.createStream(cassandraStreamName, columnList)
        streamingAPI.saveToCassandra(cassandraStreamName)
        Thread.sleep(2000)
        streamingAPI.insertData(cassandraStreamName, streamData)
        streamingAPI.stopSaveToCassandra(cassandraStreamName)
        Thread.sleep(2000)
        streamingAPI.insertData(cassandraStreamName, streamData)
        Thread.sleep(2000)
      } catch {
        case ssEx: StratioStreamingException => {
          println(ssEx.getMessage)
          fail()
        }
      }
      val storedRows = fetchStoredRowsFromCassandra(cassandraStreamName)
      cleanCassandraTable(cassandraStreamName)
      storedRows.size() should be(1)
    }

    it("should adding rows to cassandra after altering a stream", Tag("cassandra")) {
      val cassandraStreamName = "cassandrastreamtabletest3"
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.STRING)
      val secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING)
      val columnList = Seq(firstStreamColumn)
      val columnList2 = Seq(secondStreamColumn)
      val firstColumnValue = new ColumnNameValue("column1", "testValue")
      val secondColumnValue = new ColumnNameValue("column2", "testValue2")
      val streamData = Seq(firstColumnValue)
      val streamData2 = Seq(firstColumnValue, secondColumnValue)
      try {
        streamingAPI.createStream(cassandraStreamName, columnList)
        streamingAPI.saveToCassandra(cassandraStreamName)
        streamingAPI.insertData(cassandraStreamName, streamData)
        Thread.sleep(3000)
        streamingAPI.alterStream(cassandraStreamName, columnList2)
        Thread.sleep(3000)
        streamingAPI.insertData(cassandraStreamName, streamData2)
        Thread.sleep(3000)
      } catch {
        case ssEx: StratioStreamingException => fail()
      }
      val storedRows = fetchStoredRowsFromCassandra(cassandraStreamName)
      cleanCassandraTable(cassandraStreamName)
      storedRows.size() should be(2)
    }

    it("should throw a StratioEngineOperationException when the save2cassandra operation has been already defined", Tag("cassandra")) {
      val cassandraStreamName = "cassandrastreamtabletest4"
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.STRING)
      val columnList = Seq(firstStreamColumn)
      val firstColumnValue = new ColumnNameValue("column1", "testValue")
      val streamData = Seq(firstColumnValue)
      try {
        streamingAPI.createStream(cassandraStreamName, columnList)
        streamingAPI.saveToCassandra(cassandraStreamName)
        streamingAPI.insertData(cassandraStreamName, streamData)
        Thread.sleep(2000)
        intercept[StratioEngineOperationException] {
          streamingAPI.saveToCassandra(cassandraStreamName)
        }
      } catch {
        case ssEx: StratioStreamingException => fail()
      }
    }

  }

  describe("The save to mongodb operation") {
    it("should add a document to mongodb", Tag("mongodb")) {
      val mongoDBStreamName = "mongoDBTestStream1"
      cleanMongoDBTable(mongoDBStreamName)
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.STRING)
      val secondStreamColumn = new ColumnNameType("column2", ColumnType.BOOLEAN)
      val thirdStreamColumn = new ColumnNameType("column3", ColumnType.FLOAT)
      val fourthStreamColumn = new ColumnNameType("column4", ColumnType.INTEGER)
      val fifthStreamColumn = new ColumnNameType("column5", ColumnType.DOUBLE)
      val sixthStreamColumn = new ColumnNameType("column6", ColumnType.LONG)
      val columnList = Seq(firstStreamColumn,
        secondStreamColumn,
        thirdStreamColumn,
        fourthStreamColumn,
        fifthStreamColumn,
        sixthStreamColumn)
      val firstColumnValue = new ColumnNameValue("column1", "testValue")
      val secondColumnValue = new ColumnNameValue("column2", new java.lang.Boolean(true))
      val thirdColumnValue = new ColumnNameValue("column3", new java.lang.Float(2.0))
      val fourthColumnValue = new ColumnNameValue("column4", new java.lang.Integer(4))
      val fifthColumnValue = new ColumnNameValue("column5", new java.lang.Double(5))
      val sixthColumnValue = new ColumnNameValue("column6", new java.lang.Long(600000))
      val streamData = Seq(firstColumnValue,
        secondColumnValue,
        thirdColumnValue,
        fourthColumnValue,
        fifthColumnValue,
        sixthColumnValue)

      try {
        streamingAPI.createStream(mongoDBStreamName, columnList)
        streamingAPI.saveToMongo(mongoDBStreamName)
        Thread.sleep(3000)
        streamingAPI.insertData(mongoDBStreamName, streamData)
        Thread.sleep(3000)
      } catch {
        case ssEx: StratioStreamingException => fail()
      }
      val storedDocuments = fetchStoredDocumentsFromMongo(mongoDBStreamName)
      storedDocuments.count should be(1)
      val storedDocument = storedDocuments.findOne
      storedDocument.get("column1") should be("testValue")
      storedDocument.get("column2") should be(java.lang.Boolean.TRUE)
      storedDocument.get("column3") should be(2.0)
      storedDocument.get("column4") should be(4)
      storedDocument.get("column5") should be(5)
      storedDocument.get("column6") should be(600000)
      cleanMongoDBTable(mongoDBStreamName)
    }

    it("should stop adding documents to mongodb", Tag("mongodb")) {
      val mongoDBStreamName = "mongoDBTestStream2"
      cleanMongoDBTable(mongoDBStreamName)
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.STRING)
      val columnList = Seq(firstStreamColumn)
      val firstColumnValue = new ColumnNameValue("column1", "testValue")
      val streamData = Seq(firstColumnValue)
      try {
        streamingAPI.createStream(mongoDBStreamName, columnList)
        streamingAPI.saveToMongo(mongoDBStreamName)
        Thread.sleep(2000)
        streamingAPI.insertData(mongoDBStreamName, streamData)
        streamingAPI.stopSaveToMongo(mongoDBStreamName)
        Thread.sleep(2000)
        streamingAPI.insertData(mongoDBStreamName, streamData)
        Thread.sleep(2000)
      } catch {
        case ssEx: StratioStreamingException => {
          println(ssEx.getMessage)
          fail()
        }
      }
      val storedDocuments = fetchStoredDocumentsFromMongo(mongoDBStreamName)
      storedDocuments.count should be(1)
      cleanMongoDBTable(mongoDBStreamName)
    }

    it("should adding documents to mongodb after altering a stream", Tag("mongodb")) {
      val mongoDBStreamName = "mongoDBTestStream3"
      cleanMongoDBTable(mongoDBStreamName)
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.STRING)
      val secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING)
      val columnList = Seq(firstStreamColumn)
      val columnList2 = Seq(secondStreamColumn)
      val firstColumnValue = new ColumnNameValue("column1", "testValue")
      val secondColumnValue = new ColumnNameValue("column2", "testValue2")
      val streamData = Seq(firstColumnValue)
      val streamData2 = Seq(firstColumnValue, secondColumnValue)
      try {
        streamingAPI.createStream(mongoDBStreamName, columnList)
        streamingAPI.saveToMongo(mongoDBStreamName)
        streamingAPI.insertData(mongoDBStreamName, streamData)
        Thread.sleep(3000)
        streamingAPI.alterStream(mongoDBStreamName, columnList2)
        Thread.sleep(3000)
        streamingAPI.insertData(mongoDBStreamName, streamData2)
        Thread.sleep(3000)
      } catch {
        case ssEx: StratioStreamingException => fail()
      }
      val storedDocuments = fetchStoredDocumentsFromMongo(mongoDBStreamName)
      storedDocuments.count should be(2)
      val query = new BasicDBObject("column2", "testValue2")
      val cursor = storedDocuments.find(query)
      val element = cursor.next()
      element.get("column1") should be("testValue")
      element.get("column2") should be("testValue2")
      cleanMongoDBTable(mongoDBStreamName)
    }

    it("should throw a StratioEngineOperation when the save2mongo operation has been already defined", Tag("mongodb")) {
      val mongoDBStreamName = "mongoDBTestStream4"
      cleanMongoDBTable(mongoDBStreamName)
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.STRING)
      val columnList = Seq(firstStreamColumn)
      val firstColumnValue = new ColumnNameValue("column1", "testValue")
      val streamData = Seq(firstColumnValue)
      try {
        streamingAPI.createStream(mongoDBStreamName, columnList)
        streamingAPI.saveToMongo(mongoDBStreamName)
        streamingAPI.insertData(mongoDBStreamName, streamData)
        Thread.sleep(3000)
        intercept[StratioEngineOperationException] {
          streamingAPI.saveToMongo(mongoDBStreamName)
        }
      } catch {
        case ssEx: StratioStreamingException => fail()
      } finally {
        cleanMongoDBTable(mongoDBStreamName)
      }
    }
  }

  describe("The list operation") {
    it("should receive the enabled operations info", Tag("list")) {
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.STRING)
      val columnList = Seq(firstStreamColumn)
      try {
        streamingAPI.createStream(testStreamName, columnList)
        streamingAPI.listenStream(testStreamName)
        streamingAPI.saveToCassandra(testStreamName)
        streamingAPI.indexStream(testStreamName)
        Thread.sleep(3000)
        val streamInfo = streamingAPI.listStreams().get(0)
        streamInfo.getActiveActions should contain(StreamAction.LISTEN)
        streamInfo.getActiveActions should contain(StreamAction.SAVE_TO_CASSANDRA)
        streamInfo.getActiveActions should contain(StreamAction.INDEXED)
        streamingAPI.stopListenStream(testStreamName)
        streamingAPI.stopIndexStream(testStreamName)
        streamingAPI.stopSaveToCassandra(testStreamName)
        Thread.sleep(3000)
        val streamDisabledInfo = streamingAPI.listStreams().get(0)
        streamDisabledInfo.getActiveActions should not contain(StreamAction.LISTEN)
        streamDisabledInfo.getActiveActions should not contain(StreamAction.SAVE_TO_CASSANDRA)
        streamDisabledInfo.getActiveActions should not contain(StreamAction.INDEXED)
      } catch {
        case ssEx: StratioStreamingException => fail()
      }
    }
  }

  describe("The Streaming Engine") {
    it("should throw a StratioEngineStatusException when streaming engine is not running") {
      val firstStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
      val secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING)
      val columnList = Seq(firstStreamColumn, secondStreamColumn)
      removeEphemeralNode()
      //Add some delay to wait for the event to be triggered
      Thread.sleep(1000)
      intercept [StratioEngineStatusException] {
        streamingAPI.createStream(testStreamName, columnList)
      }
    }
  }

  def removeEphemeralNode() {
    zookeeperConsumer.removeZNode(ZK_EPHEMERAL_NODE_PATH)
  }

  def cleanStratioStreamingEngine() {
    userDefinedStreams.foreach(stream => streamingAPI.dropStream(stream.getStreamName))
  }

  def cleanElasticSearchIndexes() {
    val url = s"http://$elasticSearchHost:$elasticSearchPort/$elasticSearchIndex/"
    val client = HttpClientBuilder.create().build()
    val request = new HttpDelete(url)
    client.execute(request)
    client.close()
  }

  def userDefinedStreams() = {
    streamingAPI.listStreams().filterNot(stream => STATS_STREAMS.contains(stream.getStreamName))
  }

  def theNumberOfUserDefinedStreams() = {
    userDefinedStreams.size
  }

  def theNumberOfColumnsOfTheStream(streamName: String) = {
    streamingAPI.columnsFromStream(streamName).size
  }

  def theNumberOfQueriesOfTheStream(streamName: String) = {
    streamingAPI.queriesFromStream(streamName).size
  }

  def theStreamContainsTheData(data: String) = {
    val url = s"http://$elasticSearchHost:$elasticSearchPort/$elasticSearchIndex/_search"
    val client = HttpClientBuilder.create().build()
    val request = new HttpGet(url)
    val response = client.execute(request)
    val responseEntity = response.getEntity()
    val elasticSearchResponse = EntityUtils.toString(responseEntity)
    elasticSearchResponse.contains(data)
  }

  def cleanCassandraTable(streamName: String) = {
    val truncateQuery = QueryBuilder.truncate(STREAMING_KEYSPACE_NAME, streamName)
    cassandraSession.execute(truncateQuery)
  }

  def fetchStoredRowsFromCassandra(streamName: String) = {
    val selectAllQuery = QueryBuilder.select()
                  .all()
                  .from(STREAMING_KEYSPACE_NAME, streamName)
    val resultsFromCassandra = cassandraSession.execute(selectAllQuery).all()
    resultsFromCassandra
  }

  def cleanMongoDBTable(streamName: String) = {
    val collection = mongoDataBase.getCollection(streamName)
    collection.drop()
  }

  def fetchStoredDocumentsFromMongo(streamName: String) = {
    mongoDataBase.getCollection(streamName)
  }
}
