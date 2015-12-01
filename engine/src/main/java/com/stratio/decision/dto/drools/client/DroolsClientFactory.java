package com.stratio.decision.dto.drools.client;

import java.io.IOException;

import com.stratio.decision.dto.drools.configuration.model.DroolsConfigurationBean;

/**
 * Created by jmartinmenor on 13/10/15.
 */
public class DroolsClientFactory {

    public static DroolsClient getInstance(String group, DroolsConfigurationBean dc) throws IOException {

        DroolsClientImpl<?> client = new DroolsClientImpl<Object>(group, dc);
        return client;
    }

}
