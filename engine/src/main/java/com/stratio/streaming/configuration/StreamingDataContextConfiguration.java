package com.stratio.streaming.configuration;

import com.stratio.streaming.commons.constants.BUS;
import com.stratio.streaming.commons.constants.InternalTopic;
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.functions.dml.InsertIntoStreamFunction;
import com.stratio.streaming.functions.messages.FilterMessagesByOperationFunction;
import com.stratio.streaming.functions.messages.KeepPayloadFromMessageFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

import java.util.HashMap;
import java.util.Map;

@Configuration
@Import(ServiceConfiguration.class)
public class StreamingDataContextConfiguration extends StreamingContextConfiguration {

    @Bean(name = "dataContext", destroyMethod = "stop")
    @Lazy
    public JavaStreamingContext dataContext() {
        JavaStreamingContext context = this.create("stratio-streaming-data", 4041,
                configurationContext.getInternalStreamingBatchTime(), configurationContext.getInternalSparkHost());

        KeepPayloadFromMessageFunction keepPayloadFromMessageFunction = new KeepPayloadFromMessageFunction();

        Map<String, Integer> baseTopicMap = new HashMap<String, Integer>();
        baseTopicMap.put(InternalTopic.TOPIC_DATA.getTopicName(), 1);

        kafkaTopicService.createTopicIfNotExist(InternalTopic.TOPIC_DATA.getTopicName(),
                configurationContext.getKafkaReplicationFactor(), configurationContext.getKafkaPartitions());

        JavaPairDStream<String, String> messages = KafkaUtils.createStream(context,
                configurationContext.getZookeeperHostsQuorum(), InternalTopic.TOPIC_DATA.getTopicName(), baseTopicMap);

        messages.cache();

        JavaDStream<StratioStreamingMessage> insertRequests = messages.filter(
                new FilterMessagesByOperationFunction(STREAM_OPERATIONS.MANIPULATION.INSERT)).map(
                keepPayloadFromMessageFunction);

        InsertIntoStreamFunction insertIntoStreamFunction = new InsertIntoStreamFunction(streamOperationService,
                configurationContext.getZookeeperHostsQuorum());

        insertRequests.foreachRDD(insertIntoStreamFunction);

        return context;

    }

}
