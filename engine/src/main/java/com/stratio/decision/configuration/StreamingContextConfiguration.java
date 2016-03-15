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
package com.stratio.decision.configuration;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.datastax.driver.core.ProtocolOptions;
import com.stratio.decision.StreamingEngine;
import com.stratio.decision.commons.avro.Action;
import com.stratio.decision.commons.avro.ColumnType;
import com.stratio.decision.commons.avro.InsertMessage;
import com.stratio.decision.commons.constants.InternalTopic;
import com.stratio.decision.commons.constants.STREAM_OPERATIONS;
import com.stratio.decision.commons.constants.StreamAction;
import com.stratio.decision.commons.kafka.service.KafkaTopicService;
import com.stratio.decision.commons.messages.StratioStreamingMessage;
import com.stratio.decision.functions.messages.AvroDeserializeMessageFunction;
import com.stratio.decision.functions.FilterDataFunction;
import com.stratio.decision.functions.PairDataFunction;
import com.stratio.decision.functions.SaveToCassandraActionExecutionFunction;
import com.stratio.decision.functions.SaveToElasticSearchActionExecutionFunction;
import com.stratio.decision.functions.SaveToMongoActionExecutionFunction;
import com.stratio.decision.functions.SaveToSolrActionExecutionFunction;
import com.stratio.decision.functions.SendToKafkaActionExecutionFunction;
import com.stratio.decision.functions.SerializerFunction;
import com.stratio.decision.functions.dal.IndexStreamFunction;
import com.stratio.decision.functions.dal.ListenStreamFunction;
import com.stratio.decision.functions.dal.SaveToCassandraStreamFunction;
import com.stratio.decision.functions.dal.SaveToMongoStreamFunction;
import com.stratio.decision.functions.dal.SaveToSolrStreamFunction;
import com.stratio.decision.functions.dal.SendToDroolsStreamFunction;
import com.stratio.decision.functions.ddl.AddQueryToStreamFunction;
import com.stratio.decision.functions.ddl.AlterStreamFunction;
import com.stratio.decision.functions.ddl.CreateStreamFunction;
import com.stratio.decision.functions.dml.InsertIntoStreamFunction;
import com.stratio.decision.functions.dml.ListStreamsFunction;
import com.stratio.decision.functions.messages.FilterAvroMessagesByOperationFunction;
import com.stratio.decision.functions.messages.FilterMessagesByOperationFunction;
import com.stratio.decision.functions.messages.KeepPayloadFromMessageFunction;
import com.stratio.decision.serializer.impl.KafkaToJavaSerializer;
import com.stratio.decision.service.StreamOperationService;

@Configuration
@Import(ServiceConfiguration.class)
public class StreamingContextConfiguration {

    private static Logger log = LoggerFactory.getLogger(StreamingContextConfiguration.class);

    @Autowired
    private ConfigurationContext configurationContext;

    @Autowired
    private StreamOperationService streamOperationService;

    @Autowired
    private KafkaToJavaSerializer kafkaToJavaSerializer;

    private KafkaTopicService kafkaTopicService;





    private JavaStreamingContext create(String streamingContextName, int port, long streamingBatchTime, String sparkHost) {
        SparkConf conf = new SparkConf();
        conf.set("spark.ui.port", String.valueOf(port));
        conf.setAppName(streamingContextName);
        conf.setJars(JavaStreamingContext.jarOfClass(StreamingEngine.class));
        conf.setMaster(sparkHost);

        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.registerKryoClasses(new Class[] { StratioStreamingMessage.class, InsertMessage.class, ColumnType.class,
                Action.class});

        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, new Duration(streamingBatchTime));

        return streamingContext;
    }

    @Bean(name = "streamingContext", destroyMethod = "stop")
    public JavaStreamingContext streamingContext() {
        JavaStreamingContext context = this.create("stratio-streaming-context", 4040,
                configurationContext.getInternalStreamingBatchTime(), configurationContext.getInternalSparkHost());

        configureRequestContext(context);
        configureActionContext(context);
        configureDataContext(context);

        return context;
    }


    private void configureRequestContext(JavaStreamingContext context) {
        Map<String, Integer> baseTopicMap = new HashMap<>();

        baseTopicMap.put(InternalTopic.TOPIC_REQUEST.getTopicName(), 1);

        kafkaTopicService.createTopicIfNotExist(InternalTopic.TOPIC_REQUEST.getTopicName(), configurationContext.getKafkaReplicationFactor(), configurationContext.getKafkaPartitions());

        /*
        groupId must be the cluster groupId. Kafka assigns each partition of a topic to one, and one only, consumer of
        the group.
        Decision topics has only one partition (by default), so if we have two o more decision instances (consumers)
        reading the same topic with the same groupId, only one instance will be able to read from the topic
        */

        JavaPairDStream<String, String> messages = KafkaUtils.createStream(context,
                configurationContext.getZookeeperHostsQuorumWithPath(), configurationContext.getGroupId(), baseTopicMap);
        messages.cache();

        KeepPayloadFromMessageFunction keepPayloadFromMessageFunction = new KeepPayloadFromMessageFunction();
        CreateStreamFunction createStreamFunction = new CreateStreamFunction(streamOperationService,
                configurationContext.getZookeeperHostsQuorumWithPath());
        AlterStreamFunction alterStreamFunction = new AlterStreamFunction(streamOperationService,
                configurationContext.getZookeeperHostsQuorumWithPath());
        AddQueryToStreamFunction addQueryToStreamFunction = new AddQueryToStreamFunction(streamOperationService,
                configurationContext.getZookeeperHostsQuorumWithPath());
        ListenStreamFunction listenStreamFunction = new ListenStreamFunction(streamOperationService,
                configurationContext.getZookeeperHostsQuorumWithPath());
        ListStreamsFunction listStreamsFunction = new ListStreamsFunction(streamOperationService,
                configurationContext.getZookeeperHostsQuorumWithPath());


        if (configurationContext.getDroolsConfiguration() != null) {

            SendToDroolsStreamFunction sendToDroolsStreamFunction = new SendToDroolsStreamFunction
                    (streamOperationService, configurationContext.getZookeeperHostsQuorumWithPath());

            JavaDStream<StratioStreamingMessage> sendToDroolsRequests = messages.filter(
                    new FilterMessagesByOperationFunction(STREAM_OPERATIONS.ACTION.START_SENDTODROOLS)).map(
                    keepPayloadFromMessageFunction);

            JavaDStream<StratioStreamingMessage> stopSendToDroolsRequests = messages.filter(
                    new FilterMessagesByOperationFunction(STREAM_OPERATIONS.ACTION.STOP_SENDTODROOLS)).map(
                    keepPayloadFromMessageFunction);

            sendToDroolsRequests.foreachRDD(sendToDroolsStreamFunction);

            stopSendToDroolsRequests.foreachRDD(sendToDroolsStreamFunction);

        } else {
            log.warn("Drools configuration not found.");
        }


        if (configurationContext.getCassandraHosts() != null) {
            SaveToCassandraStreamFunction saveToCassandraStreamFunction = new SaveToCassandraStreamFunction(
                    streamOperationService, configurationContext.getZookeeperHostsQuorumWithPath());

            JavaDStream<StratioStreamingMessage> saveToCassandraRequests = messages.filter(
                    new FilterMessagesByOperationFunction(STREAM_OPERATIONS.ACTION.SAVETO_CASSANDRA)).map(
                    keepPayloadFromMessageFunction);

            JavaDStream<StratioStreamingMessage> stopSaveToCassandraRequests = messages.filter(
                    new FilterMessagesByOperationFunction(STREAM_OPERATIONS.ACTION.STOP_SAVETO_CASSANDRA)).map(
                    keepPayloadFromMessageFunction);

            saveToCassandraRequests.foreachRDD(saveToCassandraStreamFunction);

            stopSaveToCassandraRequests.foreachRDD(saveToCassandraStreamFunction);

        } else {
            log.warn("Cassandra configuration not found.");
        }

        if (configurationContext.getElasticSearchHosts() != null) {
            IndexStreamFunction indexStreamFunction = new IndexStreamFunction(streamOperationService,
                    configurationContext.getZookeeperHostsQuorumWithPath());

            JavaDStream<StratioStreamingMessage> streamToIndexerRequests = messages.filter(
                    new FilterMessagesByOperationFunction(STREAM_OPERATIONS.ACTION.INDEX)).map(
                    keepPayloadFromMessageFunction);

            JavaDStream<StratioStreamingMessage> stopStreamToIndexerRequests = messages.filter(
                    new FilterMessagesByOperationFunction(STREAM_OPERATIONS.ACTION.STOP_INDEX)).map(
                    keepPayloadFromMessageFunction);

            streamToIndexerRequests.foreachRDD(indexStreamFunction);

            stopStreamToIndexerRequests.foreachRDD(indexStreamFunction);
        } else {
            log.warn("Elasticsearch configuration not found.");
        }

        if (configurationContext.getSolrHost() != null) {
            SaveToSolrStreamFunction solrStreamFunction = new SaveToSolrStreamFunction(streamOperationService,
                    configurationContext.getZookeeperHostsQuorumWithPath());

            JavaDStream<StratioStreamingMessage> saveToSolrRequests = messages.filter(
                    new FilterMessagesByOperationFunction(STREAM_OPERATIONS.ACTION.SAVETO_SOLR)).map(
                    keepPayloadFromMessageFunction);

            JavaDStream<StratioStreamingMessage> stopSaveToSolrRequests = messages.filter(
                    new FilterMessagesByOperationFunction(STREAM_OPERATIONS.ACTION.STOP_SAVETO_SOLR)).map(
                    keepPayloadFromMessageFunction);

            saveToSolrRequests.foreachRDD(solrStreamFunction);

            stopSaveToSolrRequests.foreachRDD(solrStreamFunction);
        } else {
            log.warn("Solr configuration not found.");
        }

        if (configurationContext.getMongoHosts() != null) {
            SaveToMongoStreamFunction saveToMongoStreamFunction = new SaveToMongoStreamFunction(streamOperationService,
                    configurationContext.getZookeeperHostsQuorumWithPath());

            JavaDStream<StratioStreamingMessage> saveToMongoRequests = messages.filter(
                    new FilterMessagesByOperationFunction(STREAM_OPERATIONS.ACTION.SAVETO_MONGO)).map(
                    keepPayloadFromMessageFunction);

            JavaDStream<StratioStreamingMessage> stopSaveToMongoRequests = messages.filter(
                    new FilterMessagesByOperationFunction(STREAM_OPERATIONS.ACTION.STOP_SAVETO_MONGO)).map(
                    keepPayloadFromMessageFunction);

            saveToMongoRequests.foreachRDD(saveToMongoStreamFunction);

            stopSaveToMongoRequests.foreachRDD(saveToMongoStreamFunction);
        } else {
            log.warn("Mongodb configuration not found.");
        }

        // Create a DStream for each command, so we can treat all related
        // requests in the same way and also apply functions by command
        JavaDStream<StratioStreamingMessage> createRequests = messages.filter(
                new FilterMessagesByOperationFunction(STREAM_OPERATIONS.DEFINITION.CREATE)).map(
                keepPayloadFromMessageFunction);

        JavaDStream<StratioStreamingMessage> alterRequests = messages.filter(
                new FilterMessagesByOperationFunction(STREAM_OPERATIONS.DEFINITION.ALTER)).map(
                keepPayloadFromMessageFunction);

        JavaDStream<StratioStreamingMessage> addQueryRequests = messages.filter(
                new FilterMessagesByOperationFunction(STREAM_OPERATIONS.DEFINITION.ADD_QUERY)).map(
                keepPayloadFromMessageFunction);

        JavaDStream<StratioStreamingMessage> removeQueryRequests = messages.filter(
                new FilterMessagesByOperationFunction(STREAM_OPERATIONS.DEFINITION.REMOVE_QUERY)).map(
                keepPayloadFromMessageFunction);

        JavaDStream<StratioStreamingMessage> listenRequests = messages.filter(
                new FilterMessagesByOperationFunction(STREAM_OPERATIONS.ACTION.LISTEN)).map(
                keepPayloadFromMessageFunction);

        JavaDStream<StratioStreamingMessage> stopListenRequests = messages.filter(
                new FilterMessagesByOperationFunction(STREAM_OPERATIONS.ACTION.STOP_LISTEN)).map(
                keepPayloadFromMessageFunction);

        JavaDStream<StratioStreamingMessage> listRequests = messages.filter(
                new FilterMessagesByOperationFunction(STREAM_OPERATIONS.MANIPULATION.LIST)).map(
                keepPayloadFromMessageFunction);

        JavaDStream<StratioStreamingMessage> dropRequests = messages.filter(
                new FilterMessagesByOperationFunction(STREAM_OPERATIONS.DEFINITION.DROP)).map(
                keepPayloadFromMessageFunction);

        createRequests.foreachRDD(createStreamFunction);
        alterRequests.foreachRDD(alterStreamFunction);
        addQueryRequests.foreachRDD(addQueryToStreamFunction);
        removeQueryRequests.foreachRDD(addQueryToStreamFunction);
        listenRequests.foreachRDD(listenStreamFunction);
        stopListenRequests.foreachRDD(listenStreamFunction);
        listRequests.foreachRDD(listStreamsFunction);
        dropRequests.foreachRDD(createStreamFunction);

        if (configurationContext.isAuditEnabled() || configurationContext.isStatsEnabled()) {

            JavaDStream<StratioStreamingMessage> allRequests = createRequests.union(alterRequests)
                    .union(addQueryRequests).union(removeQueryRequests).union(listenRequests).union(stopListenRequests)
                    .union(listRequests).union(dropRequests);

            // TODO enable audit functionality
            // if (configurationContext.isAuditEnabled()) {
            // SaveRequestsToAuditLogFunction saveRequestsToAuditLogFunction =
            // new SaveRequestsToAuditLogFunction(
            // configurationContext.getCassandraHostsQuorum());
            //
            // // persist the RDDs to cassandra using STRATIO DEEP
            // allRequests.window(new Duration(2000), new
            // Duration(6000)).foreachRDD(saveRequestsToAuditLogFunction);
            // }
        }
    }


    private void configureActionContext(JavaStreamingContext context) {
        Map<String, Integer> baseTopicMap = new HashMap<>();


        String topicName = InternalTopic.TOPIC_ACTION.getTopicName();
        if (configurationContext.isClusteringEnabled() && configurationContext.getGroupId()!=null){
            topicName = topicName.concat("_").concat(configurationContext.getGroupId());
        }
        baseTopicMap.put(topicName, 1);

        kafkaTopicService.createTopicIfNotExist(topicName, configurationContext.getKafkaReplicationFactor(),
                configurationContext.getKafkaPartitions());

        HashMap<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("zookeeper.connect", configurationContext.getZookeeperHostsQuorumWithPath());
        kafkaParams.put("group.id", configurationContext.getGroupId());
        /*
        groupId must be the cluster groupId. Kafka assigns each partition of a topic to one, and one only, consumer of
        the group.
        Decision topics has only one partition (by default), so if we have two o more decision instances (consumers) reading the
        same topic with the same groupId, only one instance will be able to read from the topic
        */
        JavaPairDStream<String, byte[]> messages = KafkaUtils.createStream(context, String.class, byte[].class,
                kafka.serializer.StringDecoder.class, kafka.serializer.DefaultDecoder.class, kafkaParams, baseTopicMap,
                StorageLevel.MEMORY_AND_DISK_SER_2());

        AvroDeserializeMessageFunction avroDeserializeMessageFunction = new AvroDeserializeMessageFunction();
        JavaDStream<StratioStreamingMessage>  parsedDataDstream = messages.map(avroDeserializeMessageFunction);

        JavaPairDStream<StreamAction, StratioStreamingMessage> pairedDataDstream = parsedDataDstream
                .mapPartitionsToPair(new PairDataFunction());

        JavaPairDStream<StreamAction, Iterable<StratioStreamingMessage>> groupedDataDstream = pairedDataDstream
                .groupByKey();

        groupedDataDstream.cache();

        try {

            SaveToCassandraActionExecutionFunction saveToCassandraActionExecutionFunction = new SaveToCassandraActionExecutionFunction(configurationContext.getCassandraHostsQuorum(),
                    ProtocolOptions.DEFAULT_PORT, configurationContext.getCassandraMaxBatchSize(),
                    configurationContext.getCassandraBatchType());
            if (saveToCassandraActionExecutionFunction.check()) {
                log.info("Cassandra is configured properly");
                groupedDataDstream.filter(new FilterDataFunction(StreamAction.SAVE_TO_CASSANDRA)).foreachRDD(
                        saveToCassandraActionExecutionFunction);
            } else {
                log.warn("Cassandra is NOT configured properly");
            }

            SaveToMongoActionExecutionFunction saveToMongoActionExecutionFunction = new SaveToMongoActionExecutionFunction(configurationContext.getMongoHosts(),
                    configurationContext.getMongoUsername(), configurationContext
                    .getMongoPassword(), configurationContext.getMongoMaxBatchSize());
            if (saveToMongoActionExecutionFunction.check()) {
                log.info("MongoDB is configured properly");
                groupedDataDstream.filter(new FilterDataFunction(StreamAction.SAVE_TO_MONGO)).foreachRDD(
                        saveToMongoActionExecutionFunction);
            } else {
                log.warn("MongoDB is NOT configured properly");
            }

            SaveToElasticSearchActionExecutionFunction saveToElasticSearchActionExecutionFunction = new SaveToElasticSearchActionExecutionFunction(configurationContext.getElasticSearchHosts(),
                    configurationContext.getElasticSearchClusterName(), configurationContext.getElasticSearchMaxBatchSize());
            if (saveToElasticSearchActionExecutionFunction.check()) {
                log.info("ElasticSearch is configured properly");
                groupedDataDstream.filter(new FilterDataFunction(StreamAction.SAVE_TO_ELASTICSEARCH)).foreachRDD(saveToElasticSearchActionExecutionFunction);
            } else {
                log.warn("ElasticSearch is NOT configured properly");
            }

            SaveToSolrActionExecutionFunction saveToSolrActionExecutionFunction = new
                    SaveToSolrActionExecutionFunction(configurationContext.getSolrHost(), configurationContext
                    .getSolrCloudZkHost(),
                    configurationContext.getSolrCloud(),
                    configurationContext.getSolrDataDir(), configurationContext.getSolrMaxBatchSize());
            if (saveToSolrActionExecutionFunction.check()) {
                log.info("Solr is configured properly");
                groupedDataDstream.filter(new FilterDataFunction(StreamAction.SAVE_TO_SOLR)).foreachRDD(
                        saveToSolrActionExecutionFunction);
            } else {
                log.warn("Solr is NOT configured properly");
            }

            groupedDataDstream.filter(new FilterDataFunction(StreamAction.LISTEN)).foreachRDD(
                    new SendToKafkaActionExecutionFunction(configurationContext.getKafkaHostsQuorum()));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void configureDataContext(JavaStreamingContext context) {
        Map<String, Integer> baseTopicMap = new HashMap<>();


        configurationContext.getDataTopics().forEach( dataTopic -> baseTopicMap.put(dataTopic, 1));

        kafkaTopicService.createTopicsIfNotExist(configurationContext.getDataTopics(), configurationContext
                .getKafkaReplicationFactor(), configurationContext.getKafkaPartitions());

        HashMap<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("zookeeper.connect", configurationContext.getZookeeperHostsQuorumWithPath());
        kafkaParams.put("group.id", configurationContext.getGroupId());
         /*
         groupId must be the cluster groupId. Kafka assigns each partition of a topic to one, and one only, consumer of
          the group.
         Decision topics has only one partition (by default), so if we have two o more decision instances (consumers) reading the
         same topic with the same groupId, only one instance will be able to read from the topic
         */
        JavaPairDStream<String, byte[]> messages = KafkaUtils.createStream(context, String.class, byte[].class,
                kafka.serializer.StringDecoder.class, kafka.serializer.DefaultDecoder.class, kafkaParams, baseTopicMap,
                StorageLevel.MEMORY_AND_DISK_SER_2());

        AvroDeserializeMessageFunction avroDeserializeMessageFunction = new AvroDeserializeMessageFunction();
        JavaDStream<StratioStreamingMessage>  insertRequests = messages.filter(
                new FilterAvroMessagesByOperationFunction(STREAM_OPERATIONS.MANIPULATION.INSERT))
                .map(avroDeserializeMessageFunction);

        InsertIntoStreamFunction insertIntoStreamFunction = new InsertIntoStreamFunction(streamOperationService,
                configurationContext.getZookeeperHostsQuorum());
        insertRequests.foreachRDD(insertIntoStreamFunction);

    }

    @PostConstruct
    private void initTopicService() {
        kafkaTopicService = new KafkaTopicService(configurationContext.getZookeeperHostsQuorumWithPath(),
                configurationContext.getKafkaConsumerBrokerHost(), configurationContext.getKafkaConsumerBrokerPort(),
                configurationContext.getKafkaConnectionTimeout(), configurationContext.getKafkaSessionTimeout());
    }

}