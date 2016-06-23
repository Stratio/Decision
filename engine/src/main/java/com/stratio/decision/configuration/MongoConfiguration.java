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

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.stratio.decision.commons.constants.STREAMING;

/**
 * Created by josepablofernandez on 31/05/16.
 */
@Configuration
public class MongoConfiguration {

    private static Logger log = LoggerFactory.getLogger(MongoConfiguration.class);

    @Autowired
    private ConfigurationContext configurationContext;


    @Bean
    public MongoClient mongoClient(){


        List<ServerAddress> serverAddresses = new ArrayList();
        MongoClient mongoClient = null;
        try {

            for (String mongoHost : configurationContext.getMongoHosts()) {
                String[] elements = mongoHost.split(":");
                if (elements.length < 2) {
                    //no port
                    serverAddresses.add(new ServerAddress(elements[0]));
                } else {
                    serverAddresses.add(new ServerAddress(elements[0], Integer.parseInt(elements[1])));
                }
            }
            if (configurationContext.getMongoUsername() != null && configurationContext
                    .getMongoPassword() != null) {
                mongoClient = new MongoClient(serverAddresses, Arrays.asList(MongoCredential.createPlainCredential(configurationContext.getMongoUsername(),
                        "$external", configurationContext
                                .getMongoPassword().toCharArray())));
            } else {
                log.warn(
                        "MongoDB user or password are not defined. User: [{}], Password: [{}]. trying anonymous connection.",
                        configurationContext.getMongoUsername(), configurationContext
                                .getMongoPassword());
                mongoClient = new MongoClient(serverAddresses);
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        log.debug("Creating Spring Bean for mongoclient");
        return mongoClient;

    }

    @Bean
    @Lazy
    public DB mongoDB(){

        log.debug("Creating Spring Bean for mongoDB");
       return mongoClient().getDB(STREAMING.STREAMING_KEYSPACE_NAME);

    }
}
