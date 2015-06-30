//package com.stratio.streaming.configuration;
//
//import com.datastax.driver.core.ProtocolOptions;
//import com.stratio.streaming.commons.constants.BUS;
//import com.stratio.streaming.commons.constants.InternalTopic;
//import com.stratio.streaming.commons.constants.StreamAction;
//import com.stratio.streaming.commons.messages.StratioStreamingMessage;
//import com.stratio.streaming.functions.*;
//import org.apache.spark.streaming.api.java.JavaDStream;
//import org.apache.spark.streaming.api.java.JavaPairDStream;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//import org.apache.spark.streaming.kafka.KafkaUtils;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import org.springframework.context.annotation.Import;
//import org.springframework.context.annotation.Lazy;
//
//import java.util.HashMap;
//import java.util.Map;
//
//@Configuration
//@Import(ServiceConfiguration.class)
//public class StreamingProcessContextConfiguration extends StreamingContextConfiguration {
//
//    @Bean(name = "processContext", destroyMethod = "stop")
//    @Lazy
//    public JavaStreamingContext processContext() {
//        JavaStreamingContext context = this.create("stratio-streaming-process", 4042,
//                configurationContext.getStreamingBatchTime(), configurationContext.getSparkHost());
//
//        Map<String, Integer> topicActionMap = new HashMap<String, Integer>();
//        topicActionMap.put(InternalTopic.TOPIC_ACTION.getTopicName(), 1);
//
//        kafkaTopicService.createTopicIfNotExist(InternalTopic.TOPIC_REQUEST.getTopicName(),
//                configurationContext.getKafkaReplicationFactor(), configurationContext.getKafkaPartitions());
//
//        JavaPairDStream<String, String> dataDstream = KafkaUtils.createStream(context,
//                configurationContext.getZookeeperHostsQuorum(), BUS.STREAMING_GROUP_ID, topicActionMap);
//
//        JavaDStream<StratioStreamingMessage> parsedDataDstream = dataDstream.map(new SerializerFunction());
//
//        JavaPairDStream<StreamAction, StratioStreamingMessage> pairedDataDstream = parsedDataDstream
//                .mapPartitionsToPair(new PairDataFunction());
//
//        JavaPairDStream<StreamAction, Iterable<StratioStreamingMessage>> groupedDataDstream = pairedDataDstream
//                .groupByKey();
//
//        groupedDataDstream.filter(new FilterDataFunction(StreamAction.SAVE_TO_CASSANDRA)).foreachRDD(
//                new SaveToCassandraActionExecutionFunction(configurationContext.getCassandraHostsQuorum(),
//                        ProtocolOptions.DEFAULT_PORT));
//
//        groupedDataDstream.filter(new FilterDataFunction(StreamAction.SAVE_TO_MONGO)).foreachRDD(
//                new SaveToMongoActionExecutionFunction(configurationContext.getMongoHosts(),
//                        configurationContext.getMongoUsername(), configurationContext
//                        .getMongoPassword()));
//
//        groupedDataDstream.filter(new FilterDataFunction(StreamAction.INDEXED)).foreachRDD(
//                new SaveToElasticSearchActionExecutionFunction(configurationContext.getElasticSearchHosts(),
//                        configurationContext.getElasticSearchClusterName()));
//
//        groupedDataDstream.filter(new FilterDataFunction(StreamAction.LISTEN)).foreachRDD(
//                new SendToKafkaActionExecutionFunction(configurationContext.getKafkaHostsQuorum()));
//
//        return context;
//    }
//}
