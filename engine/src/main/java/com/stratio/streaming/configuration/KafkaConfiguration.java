package com.stratio.streaming.configuration;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfiguration {

    @Autowired
    private ConfigurationContext configurationContext;

    @Bean
    public Producer<String, String> producer() {
        Properties properties = new Properties();

        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("metadata.broker.list", configurationContext.getKafkaHostsQuorum());
        properties.put("producer.type", "async");

        return new Producer<String, String>(new ProducerConfig(properties));
    }

}
