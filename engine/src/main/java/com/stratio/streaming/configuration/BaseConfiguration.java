package com.stratio.streaming.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({ StreamingSiddhiConfiguration.class, StreamingContextConfiguration.class, ZookeeperConfiguration.class })
public class BaseConfiguration {

    @Bean
    public ConfigurationContext configurationContext() {
        return new ConfigurationContext();
    }

}
