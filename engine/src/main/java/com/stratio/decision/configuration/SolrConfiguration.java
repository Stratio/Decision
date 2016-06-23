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

        log.debug("Creating Spring Bean for SolrOperationsService ");


        return  new SolrOperationsService(configurationContext.getSolrHost(), configurationContext
                .getSolrCloudZkHost(), configurationContext.getSolrDataDir(), configurationContext.getSolrCloud());

    }
}
