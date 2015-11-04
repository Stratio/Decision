package com.stratio.decision.configuration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Created by aitor on 4/11/15.
 */
@Configuration
public class RulesConfiguration {

    @Autowired
    private ConfigurationContext configurationContext;

    @Bean
    public void getSession()    {
    }

}
