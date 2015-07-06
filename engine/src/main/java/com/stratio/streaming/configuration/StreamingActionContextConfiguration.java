package com.stratio.streaming.configuration;

import com.datastax.driver.core.ProtocolOptions;
import com.stratio.streaming.commons.constants.BUS;
import com.stratio.streaming.commons.constants.InternalTopic;
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS;
import com.stratio.streaming.commons.constants.StreamAction;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.functions.*;
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
public class StreamingActionContextConfiguration extends StreamingContextConfiguration {

    private static Logger log = LoggerFactory.getLogger(StreamingActionContextConfiguration.class);

    @Bean(name = "actionContext", destroyMethod = "stop")
    @Lazy
    public JavaStreamingContext processContext() {
        JavaStreamingContext context = this.create("stratio-streaming-action", 4042,
                configurationContext.getStreamingBatchTime(), configurationContext.getSparkHost());

        Map<String, Integer> topicActionMap = new HashMap<String, Integer>();
        topicActionMap.put(InternalTopic.TOPIC_ACTION.getTopicName(), 1);

        kafkaTopicService.createTopicIfNotExist(InternalTopic.TOPIC_ACTION.getTopicName(),
                configurationContext.getKafkaReplicationFactor(), configurationContext.getKafkaPartitions());

        JavaPairDStream<String, String> dataDstream = KafkaUtils.createStream(context,
                configurationContext.getZookeeperHostsQuorum(), InternalTopic.TOPIC_ACTION.getTopicName(), topicActionMap);

        JavaDStream<StratioStreamingMessage> parsedDataDstream = dataDstream.map(new SerializerFunction());

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

        return context;
    }

}
