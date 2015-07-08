package com.stratio.streaming.configuration;

import com.stratio.streaming.utils.hazelcast.StreamingHazelcastInstance;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import java.io.IOException;

/**
 * Created by mariomgal on 8/7/15.
 */
@Configuration
public class StreamingStatusDaoConfiguration {

    @Bean
    public StreamingHazelcastInstance streamingHazelcastInstance() throws IOException {
        return new StreamingHazelcastInstance();
    }
}
