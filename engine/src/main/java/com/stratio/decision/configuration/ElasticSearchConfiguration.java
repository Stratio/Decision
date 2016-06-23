/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

        for (String elasticSearchHost : configurationContext.getElasticSearchHosts()) {
            String[] elements = elasticSearchHost.split(":");
            tc.addTransportAddress(new InetSocketTransportAddress(elements[0], Integer.parseInt(elements[1])));
        }


        log.debug("Creating Spring Bean for elasticsearchClient");

        return  tc;

    }
}
