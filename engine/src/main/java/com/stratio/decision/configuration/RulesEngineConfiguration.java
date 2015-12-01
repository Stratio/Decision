package com.stratio.decision.configuration;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.stratio.decision.dto.drools.client.DroolsClient;
import com.stratio.decision.dto.drools.client.DroolsClientFactory;
import com.stratio.decision.dto.drools.configuration.DroolsHoconConfiguration;
import com.stratio.decision.dto.drools.configuration.model.DroolsConfigurationBean;

/**
 * Created by aitor on 4/11/15.
 */
@Configuration
public class RulesEngineConfiguration {

    @Autowired
    private ConfigurationContext configurationContext;
    private DroolsConfigurationBean droolsConfiguration;

    @Bean
    public DroolsClient getSession()    {
        DroolsClient client= null;
        try {
            droolsConfiguration= DroolsHoconConfiguration.getConfiguration(configurationContext);
            client= DroolsClientFactory.getInstance("default", droolsConfiguration);
            //

        } catch (IOException e) {
            e.printStackTrace();
        }
        return client;
    }

}
