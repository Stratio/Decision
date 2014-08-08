package com.stratio.streaming.configuration;

import kafka.javaapi.producer.Producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;

import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.dao.StreamStatusDao;
import com.stratio.streaming.factory.GsonFactory;
import com.stratio.streaming.serializer.Serializer;
import com.stratio.streaming.serializer.impl.JavaToSiddhiSerializer;
import com.stratio.streaming.serializer.impl.KafkaToJavaSerializer;
import com.stratio.streaming.service.CallbackService;
import com.stratio.streaming.service.StreamMetadataService;
import com.stratio.streaming.service.StreamOperationService;
import com.stratio.streaming.service.StreamStatusMetricService;

@Configuration
@Import({ DaoConfiguration.class, StreamingSiddhiConfiguration.class })
public class ServiceConfiguration {

    @Autowired
    private SiddhiManager siddhiManager;

    @Autowired
    private StreamStatusDao streamStatusDao;

    @Autowired
    private Producer<String, String> producer;

    @Bean
    public StreamOperationService streamOperationService() {
        return new StreamOperationService(siddhiManager, streamStatusDao, callbackService());
    }

    @Bean
    public StreamMetadataService streamMetadataService() {
        return new StreamMetadataService(siddhiManager);
    }

    @Bean
    public CallbackService callbackService() {
        return new CallbackService(producer, kafkaToJavaSerializer(), javaToSiddhiSerializer());
    }

    @Bean
    public Serializer<StratioStreamingMessage, Event> javaToSiddhiSerializer() {
        return new JavaToSiddhiSerializer(streamMetadataService());
    }

    @Bean
    public Serializer<String, StratioStreamingMessage> kafkaToJavaSerializer() {
        return new KafkaToJavaSerializer(GsonFactory.getInstance());
    }

    @Bean
    public StreamStatusMetricService streamStatusMetricService() {
        return new StreamStatusMetricService(streamStatusDao);
    }
}
