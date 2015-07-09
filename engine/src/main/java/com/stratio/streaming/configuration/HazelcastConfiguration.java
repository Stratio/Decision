package com.stratio.streaming.configuration;

import com.stratio.streaming.utils.hazelcast.StreamingHazelcastInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

/**
 * Created by mariomgal on 8/7/15.
 */
@Configuration
public class HazelcastConfiguration {

    @Autowired
    private ConfigurationContext configurationContext;

    @Bean
    public StreamingHazelcastInstance streamingHazelcastInstance() throws IOException {
        return new StreamingHazelcastInstance(configurationContext.getHazelcastInstanceName() + System.currentTimeMillis(),
                configurationContext.getHazelcastConfigPath());
    }
}
