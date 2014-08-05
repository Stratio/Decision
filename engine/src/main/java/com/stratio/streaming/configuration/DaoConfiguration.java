package com.stratio.streaming.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.stratio.streaming.dao.StreamStatusDao;

@Configuration
public class DaoConfiguration {

    @Bean
    public StreamStatusDao streamStatusDao() {
        return new StreamStatusDao();
    }

}
