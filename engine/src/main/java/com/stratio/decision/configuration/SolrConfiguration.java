package com.stratio.decision.configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.stratio.decision.service.SolrOperationsService;

/**
 * Created by josepablofernandez on 1/06/16.
 */
@Configuration
public class SolrConfiguration {

    private static Logger log = LoggerFactory.getLogger(CassandraConfiguration.class);

    @Autowired
    ConfigurationContext configurationContext;

    @Bean
    public SolrOperationsService solrOperationsService() {

        log.error("Creating Spring Bean for SolrOperationsService ");


        return  new SolrOperationsService(configurationContext.getSolrHost(), configurationContext
                .getSolrCloudZkHost(), configurationContext.getSolrDataDir(), configurationContext.getSolrCloud());

    }
}
