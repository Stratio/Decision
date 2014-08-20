package com.stratio.streaming.configuration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

@Configuration
public class CassandraConfiguration {

    @Autowired
    private ConfigurationContext configurationContext;

    @Bean
    @Lazy
    public Session session() {
        return Cluster.builder().addContactPoints(configurationContext.getCassandraHostsQuorum().split(",")).build()
                .connect();
    }
}
