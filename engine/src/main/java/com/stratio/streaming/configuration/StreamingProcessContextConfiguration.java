package com.stratio.streaming.configuration;

import com.stratio.streaming.commons.constants.BUS;
import com.stratio.streaming.commons.constants.InternalTopic;
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.functions.dal.IndexStreamFunction;
import com.stratio.streaming.functions.dal.ListenStreamFunction;
import com.stratio.streaming.functions.dal.SaveToCassandraStreamFunction;
import com.stratio.streaming.functions.dal.SaveToMongoStreamFunction;
import com.stratio.streaming.functions.ddl.AddQueryToStreamFunction;
import com.stratio.streaming.functions.ddl.AlterStreamFunction;
import com.stratio.streaming.functions.ddl.CreateStreamFunction;
import com.stratio.streaming.functions.dml.ListStreamsFunction;
import com.stratio.streaming.functions.messages.FilterMessagesByOperationFunction;
import com.stratio.streaming.functions.messages.KeepPayloadFromMessageFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

import java.util.HashMap;
import java.util.Map;

@Configuration
@Import(ServiceConfiguration.class)
public class StreamingProcessContextConfiguration extends StreamingContextConfiguration {

    private static Logger log = LoggerFactory.getLogger(StreamingProcessContextConfiguration.class);

    @Bean(name = "processContext", destroyMethod = "stop")
    @Lazy
    public JavaStreamingContext actionContext() {
        JavaStreamingContext context = this.create("stratio-streaming-process", 4040,
                configurationContext.getInternalStreamingBatchTime(), configurationContext.getInternalSparkHost());

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

        Map<String, Integer> baseTopicMap = new HashMap<String, Integer>();
        baseTopicMap.put(InternalTopic.TOPIC_REQUEST.getTopicName(), 1);

        kafkaTopicService.createTopicIfNotExist(InternalTopic.TOPIC_REQUEST.getTopicName(),
                configurationContext.getKafkaReplicationFactor(), configurationContext.getKafkaPartitions());

        JavaPairDStream<String, String> messages = KafkaUtils.createStream(context,
                configurationContext.getZookeeperHostsQuorum(), InternalTopic.TOPIC_REQUEST.getTopicName(), baseTopicMap);

        messages.cache();

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

        return context;
    }

}
