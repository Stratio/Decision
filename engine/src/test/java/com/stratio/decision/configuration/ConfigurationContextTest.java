package com.stratio.decision.configuration;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import com.stratio.decision.dto.drools.configuration.model.DroolsConfigurationBean;

/**
 * Created by josepablofernandez on 30/11/15.
 */
public class ConfigurationContextTest {

    private  ConfigurationContext configurationContext;

    @Before
    public void setUp() throws Exception {

        configurationContext = new ConfigurationContext();
    }

    @Test
    public void testGetDroolsConfiguration() throws Exception {

       DroolsConfigurationBean droolsConfiguration = configurationContext.getDroolsConfiguration();
       assertEquals(droolsConfiguration.getGroups().size(), 2);

       assertNotNull(droolsConfiguration.getHost());

    }


    @Test
    public void testGetCassandraHostsQuorum() throws Exception {

        String cassandraHosts = configurationContext.getCassandraHostsQuorum();
        assertNotNull(cassandraHosts);

    }


}