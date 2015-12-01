package com.stratio.decision.dto.drools.configuration;

import java.io.IOException;

import com.stratio.decision.configuration.ConfigurationContext;
import com.stratio.decision.dto.drools.configuration.model.DroolsConfigurationBean;

/**
 * Created by jmartinmenor on 8/10/15.
 */
public interface DroolsConfigurationFactory {

    DroolsConfigurationBean getConfiguration(ConfigurationContext configurationContext) throws IOException;
}
