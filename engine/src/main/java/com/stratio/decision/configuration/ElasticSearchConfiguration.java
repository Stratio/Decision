package com.stratio.decision.configuration;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Created by josepablofernandez on 1/06/16.
 */
@Configuration
public class ElasticSearchConfiguration {

    @Autowired
    ConfigurationContext configurationContext;

    private static Logger log = LoggerFactory.getLogger(ElasticSearchConfiguration.class);

    @Bean
    public Client elasticsearchClient(){

        Settings settings = ImmutableSettings.settingsBuilder()
                .put("client.transport.ignore_cluster_name", true)
                .put("cluster.name", "elasticsearch")
                .build();
        TransportClient tc = new TransportClient(settings);
//        for (String elasticSearchHost : "elasticsearch.demo.stratio.com:9300") {
//            String[] elements = elasticSearchHost.split(":");
//            tc.addTransportAddress(new InetSocketTransportAddress(elements[0], Integer.parseInt(elements[1])));
//        }

        String[] elements = "elasticsearch.demo.stratio.com:9300".split(":");
        tc.addTransportAddress(new InetSocketTransportAddress(elements[0], Integer.parseInt(elements[1])));

        log.error("Creating Spring Bean for elasticsearchClient");

        return  tc;

    }
}
