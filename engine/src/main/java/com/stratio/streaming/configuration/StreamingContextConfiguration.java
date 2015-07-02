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
package com.stratio.streaming.configuration;

import com.datastax.driver.core.ProtocolOptions;
import com.stratio.streaming.StreamingEngine;
import com.stratio.streaming.commons.constants.BUS;
import com.stratio.streaming.commons.constants.InternalTopic;
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS;
import com.stratio.streaming.commons.constants.StreamAction;
import com.stratio.streaming.commons.kafka.service.KafkaTopicService;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.functions.*;
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
import com.stratio.streaming.serializer.impl.KafkaToJavaSerializer;
import com.stratio.streaming.service.StreamOperationService;
import org.apache.spark.SparkConf;
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

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

@Configuration
@Import(ServiceConfiguration.class)
public class StreamingContextConfiguration {

    private static Logger log = LoggerFactory.getLogger(StreamingContextConfiguration.class);

    @Autowired
    protected ConfigurationContext configurationContext;

    @Autowired
    protected StreamOperationService streamOperationService;

    @Autowired
    private KafkaToJavaSerializer kafkaToJavaSerializer;

    protected KafkaTopicService kafkaTopicService;

    protected JavaStreamingContext create(String streamingContextName, int port, long streamingBatchTime, String sparkHost) {
        SparkConf conf = new SparkConf();
        conf.set("spark.ui.port", String.valueOf(port));
        conf.setAppName(streamingContextName);
        conf.setJars(JavaStreamingContext.jarOfClass(StreamingEngine.class));
        conf.setMaster(sparkHost);

        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, new Duration(streamingBatchTime));

        return streamingContext;
    }

    @Bean(name = "streamingContext", destroyMethod = "stop")
    public JavaStreamingContext streamingContext() {
        JavaStreamingContext context = this.create("stratio-streaming-context", 4040,
                configurationContext.getInternalStreamingBatchTime(), configurationContext.getInternalSparkHost());

        Map<String, Integer> baseKafkaTopics = createKafkaTopics();

        JavaPairDStream<String, String> messages = configureMessagesDStream(context, baseKafkaTopics);

        configureRequestContext(messages);
        configureActionContext(messages);
        configureDataContext(messages);
        return context;
    }

    private Map<String, Integer> createKafkaTopics() {
        Map<String, Integer> baseTopicMap = new HashMap<String, Integer>();

        baseTopicMap.put(InternalTopic.TOPIC_REQUEST.getTopicName(), 1);
        baseTopicMap.put(InternalTopic.TOPIC_ACTION.getTopicName(), 1);
        baseTopicMap.put(InternalTopic.TOPIC_DATA.getTopicName(), 1);

        kafkaTopicService.createTopicIfNotExist(InternalTopic.TOPIC_REQUEST.getTopicName(), configurationContext.getKafkaReplicationFactor(), configurationContext.getKafkaPartitions());
        kafkaTopicService.createTopicIfNotExist(InternalTopic.TOPIC_ACTION.getTopicName(), configurationContext.getKafkaReplicationFactor(), configurationContext.getKafkaPartitions());
        kafkaTopicService.createTopicIfNotExist(InternalTopic.TOPIC_DATA.getTopicName(), configurationContext.getKafkaReplicationFactor(), configurationContext.getKafkaPartitions());

        return baseTopicMap;
    }

    private JavaPairDStream<String, String> configureMessagesDStream(JavaStreamingContext context, Map<String, Integer> baseKafkaTopics) {
        JavaPairDStream<String, String> messages = KafkaUtils.createStream(context,
                configurationContext.getZookeeperHostsQuorum(), BUS.STREAMING_GROUP_ID, baseKafkaTopics);
        messages.cache();
        return messages;
    }

    private void configureRequestContext(JavaPairDStream<String, String> messages) {
        KeepPayloadFromMessageFunction keepPayloadFromMessageFunction = new KeepPayloadFromMessageFunction();
        CreateStreamFunction createStreamFunction = new CreateStreamFunction(streamOperationService,
                configurationContext.getZookeeperHostsQuorum());
        AlterStreamFunction alterStreamFunction = new AlterStreamFunction(streamOperationService,
                configurationContext.getZookeeperHostsQuorum());
        AddQueryToStreamFunction addQueryToStreamFunction = new AddQueryToStreamFunction(streamOperationService,
                configurationContext.getZookeeperHostsQuorum());
        ListenStreamFunction listenStreamFunction = new ListenStreamFunction(streamOperationService,
                configurationContext.getZookeeperHostsQuorum());
        ListStreamsFunction listStreamsFunction = new ListStreamsFunction(streamOperationService,
                configurationContext.getZookeeperHostsQuorum());
        SaveToCassandraStreamFunction saveToCassandraStreamFunction = new SaveToCassandraStreamFunction(
                streamOperationService, configurationContext.getZookeeperHostsQuorum());

        if (configurationContext.getElasticSearchHosts() != null) {
            IndexStreamFunction indexStreamFunction = new IndexStreamFunction(streamOperationService,
                    configurationContext.getZookeeperHostsQuorum());

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

        if (configurationContext.getMongoHosts() != null) {
            SaveToMongoStreamFunction saveToMongoStreamFunction = new SaveToMongoStreamFunction(streamOperationService,
                    configurationContext.getZookeeperHostsQuorum());

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

        JavaDStream<StratioStreamingMessage> saveToCassandraRequests = messages.filter(
                new FilterMessagesByOperationFunction(STREAM_OPERATIONS.ACTION.SAVETO_CASSANDRA)).map(
                keepPayloadFromMessageFunction);

        JavaDStream<StratioStreamingMessage> stopSaveToCassandraRequests = messages.filter(
                new FilterMessagesByOperationFunction(STREAM_OPERATIONS.ACTION.STOP_SAVETO_CASSANDRA)).map(
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
        saveToCassandraRequests.foreachRDD(saveToCassandraStreamFunction);
        stopSaveToCassandraRequests.foreach(saveToCassandraStreamFunction);
        listRequests.foreachRDD(listStreamsFunction);
        dropRequests.foreachRDD(createStreamFunction);

        if (configurationContext.isAuditEnabled() || configurationContext.isStatsEnabled()) {

            JavaDStream<StratioStreamingMessage> allRequests = createRequests.union(alterRequests)
                    .union(addQueryRequests).union(removeQueryRequests).union(listenRequests).union(stopListenRequests)
                    .union(saveToCassandraRequests).union(listRequests).union(dropRequests);

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

    private void configureActionContext(JavaPairDStream<String, String> messages) {

        JavaDStream<StratioStreamingMessage> parsedDataDstream = messages.map(new SerializerFunction());

        JavaPairDStream<StreamAction, StratioStreamingMessage> pairedDataDstream = parsedDataDstream
                .mapPartitionsToPair(new PairDataFunction());

        JavaPairDStream<StreamAction, Iterable<StratioStreamingMessage>> groupedDataDstream = pairedDataDstream
                .groupByKey();

        groupedDataDstream.filter(new FilterDataFunction(StreamAction.SAVE_TO_CASSANDRA)).foreachRDD(
                new SaveToCassandraActionExecutionFunction(configurationContext.getCassandraHostsQuorum(),
                        ProtocolOptions.DEFAULT_PORT));

        groupedDataDstream.filter(new FilterDataFunction(StreamAction.SAVE_TO_MONGO)).foreachRDD(
                new SaveToMongoActionExecutionFunction(configurationContext.getMongoHosts(),
                        configurationContext.getMongoUsername(), configurationContext
                        .getMongoPassword()));

        groupedDataDstream.filter(new FilterDataFunction(StreamAction.INDEXED)).foreachRDD(
                new SaveToElasticSearchActionExecutionFunction(configurationContext.getElasticSearchHosts(),
                        configurationContext.getElasticSearchClusterName()));

        groupedDataDstream.filter(new FilterDataFunction(StreamAction.LISTEN)).foreachRDD(
                new SendToKafkaActionExecutionFunction(configurationContext.getKafkaHostsQuorum()));

    }

    private void configureDataContext(JavaPairDStream<String, String> messages) {
        KeepPayloadFromMessageFunction keepPayloadFromMessageFunction = new KeepPayloadFromMessageFunction();

        JavaDStream<StratioStreamingMessage> insertRequests = messages.filter(
                new FilterMessagesByOperationFunction(STREAM_OPERATIONS.MANIPULATION.INSERT)).map(
                keepPayloadFromMessageFunction);

        InsertIntoStreamFunction insertIntoStreamFunction = new InsertIntoStreamFunction(streamOperationService,
                configurationContext.getZookeeperHostsQuorum());

        insertRequests.foreachRDD(insertIntoStreamFunction);
    }

    @PostConstruct
    private void initTopicService() {
        kafkaTopicService = new KafkaTopicService(configurationContext.getZookeeperHostsQuorum(),
                configurationContext.getKafkaConsumerBrokerHost(), configurationContext.getKafkaConsumerBrokerPort(),
                configurationContext.getKafkaConnectionTimeout(), configurationContext.getKafkaSessionTimeout());
    }

}
