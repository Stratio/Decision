//package com.stratio.streaming.integration.engine.api
//
//import com.datastax.driver.core.{Cluster, Session}
//import com.mongodb._
//import com.stratio.streaming.api.zookeeper.ZookeeperConsumer
//import com.stratio.streaming.api.{StratioStreamingAPIConfig, StratioStreamingAPIFactory}
//import com.stratio.streaming.commons.constants.STREAMING._
//import com.stratio.streaming.commons.constants._
//import com.stratio.streaming.commons.exceptions.StratioEngineOperationException
//import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
//import org.apache.curator.retry.RetryOneTime
//import org.scalatest._
//
//import scala.collection.JavaConversions._
//
///**
// * Integration tests for isConnected Stratio Streaming API call.
// */
//class StratioStreamingConnectionIntegrationTests extends FunSpec
//with StratioStreamingAPIConfig
//with ShouldMatchers
//with BeforeAndAfterEach
//with BeforeAndAfterAll {
//
//  var zookeeperCluster: String = _
//  val retryPolicy = new RetryOneTime(500)
//  var zookeeperClient: CuratorFramework = _
//  var zookeeperConsumer: ZookeeperConsumer = _
//  var streamingAPI = StratioStreamingAPIFactory.create()
//  val testStreamName = "unitTestsStream"
//  val internalTestStreamName = "stratio_"
//  val elasticSearchIndex = "stratiostreaming"
//  var elasticSearchHost = ""
//  var elasticSearchPort = ""
//  var cassandraHost = ""
//  var mongoHost = ""
//  val internalStreamName = STREAMING.STATS_NAMES.STATS_STREAMS(0)
//  var mongoClient: MongoClient = _
//  var mongoDataBase: DB = _
//  var cassandraCluster: Cluster = _
//  var cassandraSession: Session = _
//
//  override def beforeAll(conf: ConfigMap) {
//    if (configurationHasBeenDefinedThroughCommandLine(conf)) {
//      val zookeeperHost = conf.get("zookeeperHost").get.toString
//      val zookeeperPort = conf.get("zookeeperPort").get.toString.toInt
//      val kafkaHost = conf.get("kafkaHost").get.toString
//      val kafkaPort = conf.get("kafkaPort").get.toString.toInt
//
//      elasticSearchHost = conf.get("elasticSearchHost").get.toString
//      elasticSearchPort = conf.get("elasticSearchPort").get.toString
//
//      zookeeperCluster = s"$zookeeperHost:$zookeeperPort"
//      zookeeperClient = CuratorFrameworkFactory.newClient(zookeeperCluster, retryPolicy)
//      zookeeperConsumer = new ZookeeperConsumer(zookeeperClient)
//      cassandraHost = conf.get("cassandraHost").get.toString
//      mongoHost = conf.get("mongoHost").get.toString
//    } else {
//      //Pickup the config from stratio-streaming.conf
//      zookeeperCluster = config.getString("zookeeper.server") + ":" + config.getString("zookeeper.port")
//      zookeeperClient = CuratorFrameworkFactory.newClient(zookeeperCluster, retryPolicy)
//      zookeeperConsumer = new ZookeeperConsumer(zookeeperClient)
//      elasticSearchHost = config.getString("elasticsearch.server")
//      elasticSearchPort = config.getString("elasticsearch.port")
//      cassandraHost = config.getString("cassandra.host")
//      mongoHost = config.getString("mongo.host")
//      streamingAPI.init()
//    }
//    zookeeperClient.start()
//    cassandraCluster = Cluster.builder()
//      .addContactPoint(cassandraHost)
//      .build()
//    cassandraSession = cassandraCluster.connect()
//    mongoClient = new MongoClient(mongoHost)
//    mongoDataBase = mongoClient.getDB(STREAMING_KEYSPACE_NAME)
//    checkStatusAndCleanTheEngine()
//  }
//
//  override def afterAll() {
//    cassandraSession.close()
//    mongoDataBase.dropDatabase()
//    mongoClient.close()
//  }
//
//  def configurationHasBeenDefinedThroughCommandLine(conf: ConfigMap) = {
//    conf.get("zookeeperHost").isDefined &&
//      conf.get("zookeeperPort").isDefined &&
//      conf.get("kafkaHost").isDefined &&
//      conf.get("kafkaPort").isDefined &&
//      conf.get("elasticSearchHost").isDefined &&
//      conf.get("elasticSearchPort").isDefined &&
//      conf.get("cassandraHost").isDefined &&
//      conf.get("mongoHost").isDefined
//  }
//
//  def checkStatusAndCleanTheEngine() {
//    if (!zookeeperConsumer.zNodeExists(ZK_EPHEMERAL_NODE_PATH)) {
//      zookeeperClient.create().forPath(ZK_EPHEMERAL_NODE_PATH)
//      //Delay to get rid of flakiness
//      Thread.sleep(2000)
//    }
//    cleanStratioStreamingEngine()
//  }
//
//  def userDefinedStreams() = {
//    streamingAPI.listStreams().filter(stream => stream.getUserDefined())
//  }
//
//  def cleanStratioStreamingEngine() {
//    try {
//      userDefinedStreams.foreach(stream => streamingAPI.dropStream(stream.getStreamName))
//    } catch {
//      case ex: StratioEngineOperationException => //Avoid non-existing streams problems
//    }
//  }
//
//  describe("The isConnected operation") {
//    it("should return true when streaming is up and running") {
//      streamingAPI.isConnected() should be(true)
//    }
//    it("should return false when streaming is disconnected") {
//      streamingAPI.close
//      streamingAPI.isConnected() should be(false)
//    }
//  }
//
//}
