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
package com.stratio.streaming;

import java.net.MalformedURLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import com.google.common.net.HostAndPort;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.stratio.streaming.commons.constants.BUS;
import com.stratio.streaming.commons.constants.STREAMING;
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS;
import com.stratio.streaming.commons.constants.StreamAction;
import com.stratio.streaming.commons.kafka.service.KafkaTopicService;
import com.stratio.streaming.commons.kafka.service.TopicService;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.functions.dal.IndexStreamFunction;
import com.stratio.streaming.functions.dal.ListenStreamFunction;
import com.stratio.streaming.functions.dal.SaveToCassandraStreamFunction;
import com.stratio.streaming.functions.dal.SaveToMongoStreamFunction;
import com.stratio.streaming.functions.ddl.AddQueryToStreamFunction;
import com.stratio.streaming.functions.ddl.AlterStreamFunction;
import com.stratio.streaming.functions.ddl.CreateStreamFunction;
import com.stratio.streaming.functions.dml.InsertIntoStreamFunction;
import com.stratio.streaming.functions.dml.ListStreamsFunction;
import com.stratio.streaming.functions.messages.FilterMessagesByOperationFunction;
import com.stratio.streaming.functions.messages.KeepPayloadFromMessageFunction;
import com.stratio.streaming.functions.requests.CollectRequestForStatsFunction;
import com.stratio.streaming.functions.requests.SaveRequestsToAuditLogFunction;
import com.stratio.streaming.streams.QueryDTO;
import com.stratio.streaming.streams.StreamPersistence;
import com.stratio.streaming.streams.StreamSharedStatus;
import com.stratio.streaming.utils.SiddhiUtils;
import com.stratio.streaming.utils.ZKUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * @author dmorales
 * 
 *         ================= Stratio Streaming =================
 * 
 *         1) Run a global Siddhi CEP engine 2) Listen to the Kafka topic in
 *         order to receive stream commands (CREATE, ADDQUERY, LIST, DROP,
 *         INSERT, LISTEN, ALTER) 3) Execute commands and send ACKs to Zookeeper
 *         4) Send back the events if there are listeners
 * 
 */
public class StreamingEngine {

    private static Logger logger = LoggerFactory.getLogger(StreamingEngine.class);
    private static SiddhiManager siddhiManager;
    private static String cassandraCluster;
    private static Boolean failOverEnabled;
    private static JavaStreamingContext jssc;

    /**
     * @param args
     * @throws MalformedURLException
     * @throws Exception
     */
    public static void main(String[] args) throws MalformedURLException {

        Config config = loadConfig();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {

                logger.info("Shutting down Stratio Streaming..");

                // shutdown spark
                if (jssc != null) {
                    jssc.stop();
                }

                // shutdown siddhi
                if (siddhiManager != null) {

                    // remove All revisions (HA)
                    StreamPersistence.removeEngineStatusFromCleanExit(getSiddhiManager());

                    // shutdown listeners

                    try {

                        getSiddhiManager().getSiddhiContext().getHazelcastInstance()
                                .getTopic(STREAMING.INTERNAL_LISTEN_TOPIC).publish("*");
                        getSiddhiManager().getSiddhiContext().getHazelcastInstance()
                                .getTopic(STREAMING.INTERNAL_SAVE2CASSANDRA_TOPIC).publish("*");

                    } catch (HazelcastInstanceNotActiveException notActive) {
                        logger.info("Hazelcast is not active at this moment");
                    }

                    getSiddhiManager().shutdown();
                }

                // shutdown zookeeper
                ZKUtils.shutdownZKUtils();

                logger.info("Shutdown complete, bye�");
            }
        });

        try {
            launchStratioStreamingEngine(config);
        } catch (Exception e) {
            logger.error("General error: " + e.getMessage() + " // " + e.getClass(), e);
        }

    }

    /**
     * 
     * - Launch the main process: spark context, kafkaInputDstream and siddhi
     * CEP engine - Filter the incoming messages (kafka) by key in order to
     * separate the different commands - Parse the request contained in the
     * payload - execute related command for each request
     * 
     * 
     * 
     * @param sparkMaster
     * @param zkCluster
     * @param kafkaCluster
     * @param topics
     * @throws Exception
     */
    private static void launchStratioStreamingEngine(Config config) throws Exception {

        cassandraCluster = config.getString("cassandra.host");
        failOverEnabled = config.getBoolean("failOverEnabled");
        String kafkaCluster = config.getString("kafka.host");
        String zkCluster = config.getString("zookeeper.host");

        boolean enableAuditing = config.getBoolean("auditEnabled");
        boolean enableStats = config.getBoolean("statsEnabled");
        boolean printStreams = config.getBoolean("printStreams");

        long streamingBatchTime = config.getDuration("spark.streamingBatchTime", TimeUnit.MILLISECONDS);

        String topics = BUS.TOPICS;

        ZKUtils.getZKUtils(zkCluster).createEphemeralZNode(STREAMING.ZK_BASE_PATH + "/" + "engine",
                String.valueOf(System.currentTimeMillis()).getBytes());

        // Create the context with a x seconds batch size
        // jssc = new JavaStreamingContext(sparkMaster,
        // StreamingEngine.class.getName(),
        // new Duration(STREAMING.DURATION_MS), System.getenv("SPARK_HOME"),
        // JavaStreamingContext.jarOfClass(StreamingEngine.class));
        jssc = new JavaStreamingContext(config.getString("spark.host"), StreamingEngine.class.getName(), new Duration(
                streamingBatchTime));
        jssc.sparkContext().getConf().setJars(JavaStreamingContext.jarOfClass(StreamingEngine.class));

        KeepPayloadFromMessageFunction keepPayloadFromMessageFunction = new KeepPayloadFromMessageFunction();
        CreateStreamFunction createStreamFunction = new CreateStreamFunction(getSiddhiManager(), zkCluster);
        AlterStreamFunction alterStreamFunction = new AlterStreamFunction(getSiddhiManager(), zkCluster);
        InsertIntoStreamFunction insertIntoStreamFunction = new InsertIntoStreamFunction(getSiddhiManager(), zkCluster);
        AddQueryToStreamFunction addQueryToStreamFunction = new AddQueryToStreamFunction(getSiddhiManager(), zkCluster);
        ListenStreamFunction listenStreamFunction = new ListenStreamFunction(getSiddhiManager(), zkCluster,
                kafkaCluster);
        CollectRequestForStatsFunction collectRequestForStatsFunction = new CollectRequestForStatsFunction(
                getSiddhiManager(), zkCluster, kafkaCluster);
        ListStreamsFunction listStreamsFunction = new ListStreamsFunction(getSiddhiManager(), zkCluster);
        SaveRequestsToAuditLogFunction saveRequestsToAuditLogFunction = new SaveRequestsToAuditLogFunction(
                getSiddhiManager(), zkCluster, kafkaCluster, cassandraCluster, enableAuditing);
        SaveToCassandraStreamFunction saveToCassandraStreamFunction = new SaveToCassandraStreamFunction(
                getSiddhiManager(), zkCluster, cassandraCluster);

        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        String[] topic_list = topics.split(",");

        // building the topic map, by using the num of partitions of each topic
        HostAndPort kafkaConnectionData = HostAndPort.fromString(kafkaCluster);
        TopicService topicService = new KafkaTopicService(zkCluster, kafkaConnectionData.getHostText(),
                kafkaConnectionData.getPortOrDefault(9092), config.getInt("kafka.connectionTimeout"),
                config.getInt("kafka.sessionTimeout"));
        for (String topic : topic_list) {
            topicService.createTopicIfNotExist(topic, config.getInt("kafka.replicationFactor"),
                    config.getInt("kafka.partitions"));
            Integer partitions = topicService.getNumPartitionsForTopic(topic);
            if (partitions == 0) {
                partitions = config.getInt("kafka.partitions");
            }
            topicMap.put(topic, partitions);
        }

        // Start the Kafka stream
        JavaPairDStream<String, String> messages = KafkaUtils.createStream(jssc, zkCluster, BUS.STREAMING_GROUP_ID,
                topicMap);

        // as we are using messages several times, the best option is to cache
        // it
        messages.cache();

        if (config.hasPath("elasticsearch")) {
            HostAndPort elasticSearchConnectionData = HostAndPort.fromString(config.getString("elasticsearch.host"));
            IndexStreamFunction indexStreamFunction = new IndexStreamFunction(getSiddhiManager(), zkCluster,
                    elasticSearchConnectionData.getHostText(), elasticSearchConnectionData.getPortOrDefault(9300));

            JavaDStream<StratioStreamingMessage> streamToIndexer_requests = messages.filter(
                    new FilterMessagesByOperationFunction(STREAM_OPERATIONS.ACTION.INDEX)).map(
                    keepPayloadFromMessageFunction);

            JavaDStream<StratioStreamingMessage> stopStreamToIndexer_requests = messages.filter(
                    new FilterMessagesByOperationFunction(STREAM_OPERATIONS.ACTION.STOP_INDEX)).map(
                    keepPayloadFromMessageFunction);

            streamToIndexer_requests.foreachRDD(indexStreamFunction);

            stopStreamToIndexer_requests.foreachRDD(indexStreamFunction);
        } else {
            logger.warn("Elasticsearch configuration not found.");
        }

        if (config.hasPath("mongo")) {
            SaveToMongoStreamFunction saveToMongoStreamFunction = new SaveToMongoStreamFunction(getSiddhiManager(),
                    zkCluster, config.getString("mongo.host"), config.getInt("mongo.port"), (String) valueOrNull(
                            config, "mongo.username"), (String) valueOrNull(config, "mongo.password"));

            JavaDStream<StratioStreamingMessage> saveToMongo_requests = messages.filter(
                    new FilterMessagesByOperationFunction(STREAM_OPERATIONS.ACTION.SAVETO_MONGO)).map(
                    keepPayloadFromMessageFunction);

            JavaDStream<StratioStreamingMessage> stop_saveToMongo_requests = messages.filter(
                    new FilterMessagesByOperationFunction(STREAM_OPERATIONS.ACTION.STOP_SAVETO_MONGO)).map(
                    keepPayloadFromMessageFunction);

            saveToMongo_requests.foreachRDD(saveToMongoStreamFunction);

            stop_saveToMongo_requests.foreach(saveToMongoStreamFunction);
        } else {
            logger.warn("Mongodb configuration not found.");
        }

        // Create a DStream for each command, so we can treat all related
        // requests in the same way and also apply functions by command
        JavaDStream<StratioStreamingMessage> create_requests = messages.filter(
                new FilterMessagesByOperationFunction(STREAM_OPERATIONS.DEFINITION.CREATE)).map(
                keepPayloadFromMessageFunction);

        JavaDStream<StratioStreamingMessage> alter_requests = messages.filter(
                new FilterMessagesByOperationFunction(STREAM_OPERATIONS.DEFINITION.ALTER)).map(
                keepPayloadFromMessageFunction);

        JavaDStream<StratioStreamingMessage> insert_requests = messages.filter(
                new FilterMessagesByOperationFunction(STREAM_OPERATIONS.MANIPULATION.INSERT)).map(
                keepPayloadFromMessageFunction);

        JavaDStream<StratioStreamingMessage> add_query_requests = messages.filter(
                new FilterMessagesByOperationFunction(STREAM_OPERATIONS.DEFINITION.ADD_QUERY)).map(
                keepPayloadFromMessageFunction);

        JavaDStream<StratioStreamingMessage> remove_query_requests = messages.filter(
                new FilterMessagesByOperationFunction(STREAM_OPERATIONS.DEFINITION.REMOVE_QUERY)).map(
                keepPayloadFromMessageFunction);

        JavaDStream<StratioStreamingMessage> listen_requests = messages.filter(
                new FilterMessagesByOperationFunction(STREAM_OPERATIONS.ACTION.LISTEN)).map(
                keepPayloadFromMessageFunction);

        JavaDStream<StratioStreamingMessage> stop_listen_requests = messages.filter(
                new FilterMessagesByOperationFunction(STREAM_OPERATIONS.ACTION.STOP_LISTEN)).map(
                keepPayloadFromMessageFunction);

        JavaDStream<StratioStreamingMessage> saveToCassandra_requests = messages.filter(
                new FilterMessagesByOperationFunction(STREAM_OPERATIONS.ACTION.SAVETO_CASSANDRA)).map(
                keepPayloadFromMessageFunction);

        JavaDStream<StratioStreamingMessage> stop_saveToCassandra_requests = messages.filter(
                new FilterMessagesByOperationFunction(STREAM_OPERATIONS.ACTION.STOP_SAVETO_CASSANDRA)).map(
                keepPayloadFromMessageFunction);

        JavaDStream<StratioStreamingMessage> list_requests = messages.filter(
                new FilterMessagesByOperationFunction(STREAM_OPERATIONS.MANIPULATION.LIST)).map(
                keepPayloadFromMessageFunction);

        JavaDStream<StratioStreamingMessage> drop_requests = messages.filter(
                new FilterMessagesByOperationFunction(STREAM_OPERATIONS.DEFINITION.DROP)).map(
                keepPayloadFromMessageFunction);

        create_requests.foreachRDD(createStreamFunction);

        alter_requests.foreachRDD(alterStreamFunction);

        insert_requests.foreachRDD(insertIntoStreamFunction);

        add_query_requests.foreachRDD(addQueryToStreamFunction);

        remove_query_requests.foreachRDD(addQueryToStreamFunction);

        listen_requests.foreachRDD(listenStreamFunction);

        stop_listen_requests.foreachRDD(listenStreamFunction);

        saveToCassandra_requests.foreachRDD(saveToCassandraStreamFunction);

        stop_saveToCassandra_requests.foreach(saveToCassandraStreamFunction);

        list_requests.foreachRDD(listStreamsFunction);

        drop_requests.foreachRDD(createStreamFunction);

        if (enableAuditing || enableStats) {

            JavaDStream<StratioStreamingMessage> allRequests = create_requests.union(alter_requests)
                    .union(insert_requests).union(add_query_requests).union(remove_query_requests)
                    .union(listen_requests).union(stop_listen_requests).union(saveToCassandra_requests)
                    .union(list_requests).union(drop_requests);

            if (enableAuditing) {
                // persist the RDDs to cassandra using STRATIO DEEP
                allRequests.foreachRDD(saveRequestsToAuditLogFunction);
            }

            if (enableStats) {
                allRequests.foreachRDD(collectRequestForStatsFunction);

            }
        }

        StreamPersistence.saveStreamingEngineStatus(getSiddhiManager());

        if (printStreams) {

            // DEBUG STRATIO STREAMING ENGINE //
            messages.count().foreach(new Function<JavaRDD<Long>, Void>() {

                private static final long serialVersionUID = -2371501158355376325L;

                @Override
                public Void call(JavaRDD<Long> arg0) throws Exception {
                    StringBuffer sb = new StringBuffer();
                    sb.append("\n********************************************\n");
                    sb.append("**            SIDDHI STREAMS              **\n");
                    sb.append("** countSiddhi:");
                    sb.append(siddhiManager.getStreamDefinitions().size());
                    sb.append(" // countHazelcast: ");
                    sb.append(getSiddhiManager().getSiddhiContext().getHazelcastInstance()
                            .getMap(STREAMING.STREAM_STATUS_MAP).size());
                    sb.append("     **\n");

                    for (StreamDefinition streamMetaData : getSiddhiManager().getStreamDefinitions()) {

                        StringBuffer streamDefinition = new StringBuffer();

                        streamDefinition.append(streamMetaData.getStreamId());

                        for (Attribute column : streamMetaData.getAttributeList()) {
                            streamDefinition.append(" |" + column.getName() + "," + column.getType());
                        }

                        if (StreamSharedStatus.getStreamStatus(streamMetaData.getStreamId(), getSiddhiManager()) != null) {
                            HashMap<String, QueryDTO> attachedQueries = StreamSharedStatus.getStreamStatus(
                                    streamMetaData.getStreamId(), getSiddhiManager()).getAddedQueries();

                            streamDefinition.append(" /// " + attachedQueries.size() + " attachedQueries: (");

                            for (String queryId : attachedQueries.keySet()) {
                                streamDefinition.append(queryId + "/");
                            }

                            streamDefinition.append(" - userDefined:"
                                    + StreamSharedStatus.getStreamStatus(streamMetaData.getStreamId(),
                                            getSiddhiManager()).isUserDefined() + "- ");
                            streamDefinition.append(" - listenEnable:"
                                    + StreamSharedStatus.getStreamStatus(streamMetaData.getStreamId(),
                                            getSiddhiManager()).isActionEnabled(StreamAction.LISTEN) + "- ");
                        }

                        sb.append("** stream: ".concat(streamDefinition.toString()).concat("\n"));
                    }

                    sb.append("********************************************\n");

                    logger.info(sb.toString());

                    StreamPersistence.saveStreamingEngineStatus(getSiddhiManager());

                    return null;
                }

            });

            // messages.print();

        }

        jssc.start();
        logger.info("Stratio streaming started at {}", new Date());
        jssc.awaitTermination();

    }

    private static SiddhiManager getSiddhiManager() {
        if (siddhiManager == null) {
            siddhiManager = SiddhiUtils.setupSiddhiManager(cassandraCluster, failOverEnabled);
        }

        return siddhiManager;
    }

    private static Config loadConfig() throws MalformedURLException {
        Config conf = ConfigFactory.load("config");
        return conf;
    }

    // TODO refactor
    private static Object valueOrNull(Config config, String key) {
        if (config.hasPath(key)) {
            return config.getAnyRef(key);
        } else {
            return null;
        }
    }
}
