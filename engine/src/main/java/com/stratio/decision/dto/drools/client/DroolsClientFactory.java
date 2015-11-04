package com.stratio.decision.dto.drools.client;

import java.io.IOException;

import com.stratio.decision.dto.drools.configuration.DroolsConfigurationFactory;
import com.stratio.decision.dto.drools.configuration.DroolsPropertiesConfigurationFactoryImpl;
import com.stratio.decision.dto.drools.configuration.model.DroolsConfiguration;

/**
 * Created by jmartinmenor on 13/10/15.
 */
public class DroolsClientFactory {


    public DroolsClient getInstance(String group) throws IOException {

        DroolsConfigurationFactory dcf = new DroolsPropertiesConfigurationFactoryImpl();
        DroolsConfiguration dc = dcf.getConfiguration();


        return this.getInstance(dc, group);

    }

    public DroolsClient getInstance(DroolsConfiguration dc, String group) throws IOException {


        DroolsClientImpl<?> client = new DroolsClientImpl<Object>(dc, group);



        return client;
    }

}
