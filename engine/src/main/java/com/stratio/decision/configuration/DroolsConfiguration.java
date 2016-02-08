package com.stratio.decision.configuration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import com.stratio.decision.drools.DroolsConnectionContainer;

/**
 * Created by josepablofernandez on 30/11/15.
 */
@Configuration
public class DroolsConfiguration {

    @Autowired
    private ConfigurationContext configurationContext;

    @Bean
    @Lazy
    public DroolsConnectionContainer droolsConnectionContainer() {

        return new DroolsConnectionContainer(configurationContext.getDroolsConfiguration());

    }


}
