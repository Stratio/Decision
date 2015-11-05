package com.stratio.decision.dto.drools.configuration;

import java.io.IOException;

import com.stratio.decision.configuration.ConfigurationContext;
import com.stratio.decision.dto.drools.configuration.model.DroolsConfiguration;

/**
 * Created by jmartinmenor on 8/10/15.
 */
public interface DroolsConfigurationFactory {

    DroolsConfiguration getConfiguration(ConfigurationContext configurationContext) throws IOException;
}
