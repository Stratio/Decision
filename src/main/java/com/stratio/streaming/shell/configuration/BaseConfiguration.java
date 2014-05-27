package com.stratio.streaming.shell.configuration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

import com.stratio.streaming.api.IStratioStreamingAPI;
import com.stratio.streaming.shell.dao.CachedStreamsDAO;
import com.stratio.streaming.shell.dao.impl.CachedStreamsDAOImpl;

@Configuration
@Import({ StreamingApiConfiguration.class, CacheConfiguration.class })
public class BaseConfiguration {

    @Autowired
    private IStratioStreamingAPI stratioStreamingApi;

    @Bean
    public CachedStreamsDAO cachedStreamsDao() {
        return new CachedStreamsDAOImpl(stratioStreamingApi);
    }

    @Bean
    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }

}
