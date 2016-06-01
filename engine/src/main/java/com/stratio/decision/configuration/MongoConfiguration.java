package com.stratio.decision.configuration;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import com.mongodb.DB;
import com.mongodb.MongoClient;
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

    /*

    mongo.demo.stratio.com:27017
    private MongoClient getMongoClient() throws UnknownHostException {
        if (mongoClient == null) {
            List<ServerAddress> serverAddresses = new ArrayList();
            for (String mongoHost : mongoHosts) {
                String[] elements = mongoHost.split(":");
                if (elements.length < 2) {
                    //no port
                    serverAddresses.add(new ServerAddress(elements[0]));
                } else {
                    serverAddresses.add(new ServerAddress(elements[0], Integer.parseInt(elements[1])));
                }
            }
            if (username != null && password != null) {
                mongoClient = new MongoClient(serverAddresses, Arrays.asList(MongoCredential.createPlainCredential(username,
                        "$external", password.toCharArray())));
            } else {
                log.warn(
                        "MongoDB user or password are not defined. User: [{}], Password: [{}]. trying anonymous connection.",
                        username, password);
                mongoClient = new MongoClient(serverAddresses);
            }
        }
        return mongoClient;
    }

    private DB getDB() throws UnknownHostException {
        if (streamingDb == null) {
            streamingDb = getMongoClient().getDB(STREAMING.STREAMING_KEYSPACE_NAME);
        }
        return streamingDb;
    }

     */

    @Bean
    public MongoClient mongoClient(){

        List<ServerAddress> serverAddresses = new ArrayList();
        MongoClient mongoClient = null;

        try {
            serverAddresses.add(new ServerAddress("mongo.demo.stratio.com", 27017));
            mongoClient = new MongoClient(serverAddresses);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }


        log.error("Creating Spring Bean for mongoclient");
        return mongoClient;

    }

    @Bean
    @Lazy
    public DB mongoDB(){

        log.error("Creating Spring Bean for mongoDB");
       return mongoClient().getDB(STREAMING.STREAMING_KEYSPACE_NAME);

    }
}
