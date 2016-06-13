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
import org.springframework.context.annotation.Lazy;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.stratio.decision.functions.SaveToCassandraActionExecutionFunction;
import com.stratio.decision.service.SaveToCassandraOperationsService;

@Configuration
public class CassandraConfiguration {

    @Autowired
    private ConfigurationContext configurationContext;

    private static Logger log = LoggerFactory.getLogger(CassandraConfiguration.class);


    @Bean
    public SaveToCassandraOperationsService saveToCassandraOperationsService() {

        log.debug("Creating Spring Bean for SaveToCassandraOperationsService");

        return new SaveToCassandraOperationsService(cassandraSession());
    }


    @Bean
    public Session cassandraSession() {

        log.debug("Creating Spring Bean for cassandra session");
        Session session;

        try {
            session = Cluster.builder().addContactPoints(configurationContext.getCassandraHostsQuorum().split(","))
                    .withPort(9042)
                    .build().connect();
        } catch (Exception e) {
            session = null;
        }

        return session;

    }

}
