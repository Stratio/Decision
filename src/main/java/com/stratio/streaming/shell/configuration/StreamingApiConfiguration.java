package com.stratio.streaming.shell.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import com.stratio.streaming.api.IStratioStreamingAPI;
import com.stratio.streaming.api.StratioStreamingAPIFactory;
import com.stratio.streaming.commons.exceptions.StratioEngineConnectionException;

@Configuration
@PropertySource("classpath:shell.properties")
@EnableCaching
public class StreamingApiConfiguration {

    @Value("${kafka.host}")
    private String kafkaHost;

    @Value("${kafka.port}")
    private Integer kafkaPort;

    @Value("${zookeeper.host}")
    private String zookeeperHost;

    @Value("${zookeeper.port}")
    private Integer zookeeperPort;

    @Bean
    public IStratioStreamingAPI stratioStreamingApi() {
        try {
            return StratioStreamingAPIFactory.create().initializeWithServerConfig(kafkaHost, kafkaPort, zookeeperHost,
                    zookeeperPort);
        } catch (StratioEngineConnectionException e) {
            throw new RuntimeException("Problem creating streming api", e);
        }
    }
}
