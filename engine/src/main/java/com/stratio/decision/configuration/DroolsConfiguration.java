package com.stratio.decision.configuration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;

import com.stratio.decision.dto.drools.configuration.model.DroolsConfigurationBean;

/**
 * Created by josepablofernandez on 30/11/15.
 */
public class DroolsConfiguration {

    @Autowired
    private ConfigurationContext configurationContext;

    @Bean
    @Lazy
    public DroolsConfigurationBean getDroolsConfiguration() {

        return configurationContext.getDroolsConfiguration();

    }
}
