/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.decision.functions;

import com.google.common.collect.Iterables;
import com.mongodb.*;
import com.stratio.decision.commons.constants.STREAMING;
import com.stratio.decision.commons.messages.ColumnNameTypeValue;
import com.stratio.decision.commons.messages.StratioStreamingMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.*;
import java.util.Map.Entry;

public class SaveToMongoActionExecutionFunction extends BaseActionExecutionFunction {

    private static final long serialVersionUID = -7890478075965866737L;

    private static Logger log = LoggerFactory.getLogger(SaveToMongoActionExecutionFunction.class);

    private MongoClient mongoClient;
    private DB streamingDb;

    private final List<String> mongoHosts;
    private final String username;
    private final String password;
    private final Integer maxBatchSize;


    public SaveToMongoActionExecutionFunction(List<String> mongoHosts, String username, String password, Integer
            maxBatchSize) {
        this.mongoHosts = mongoHosts;
        this.username = username;
        this.password = password;
        this.maxBatchSize = maxBatchSize;
    }


    @Override
    public void process(Iterable<StratioStreamingMessage> messages) throws Exception {



        Integer partitionSize = maxBatchSize;

        if (partitionSize <= 0){
            partitionSize = Iterables.size(messages);
        }

        Iterable<List<StratioStreamingMessage>> partitionIterables =  Iterables.partition(messages, partitionSize);

        try {

            for (List<StratioStreamingMessage> messageList : partitionIterables) {

                Map<String, BulkWriteOperation> elementsToInsert = new HashMap<String, BulkWriteOperation>();

                for (StratioStreamingMessage event : messageList) {
                    BasicDBObject object = new BasicDBObject(TIMESTAMP_FIELD, event.getTimestamp());
                    for (ColumnNameTypeValue columnNameTypeValue : event.getColumns()) {
                        object.append(columnNameTypeValue.getColumn(), columnNameTypeValue.getValue());
                    }

                    BulkWriteOperation bulkInsertOperation = elementsToInsert.get(event.getStreamName());

                    if (bulkInsertOperation == null) {
                        bulkInsertOperation = getDB().getCollection(event.getStreamName())
                                .initializeUnorderedBulkOperation();

                        elementsToInsert.put(event.getStreamName(), bulkInsertOperation);
                        getDB().getCollection(event.getStreamName())
                                .createIndex(new BasicDBObject(TIMESTAMP_FIELD, -1));
                    }

                    bulkInsertOperation.insert(object);
                }

                for (Entry<String, BulkWriteOperation> stratioStreamingMessage : elementsToInsert.entrySet()) {
                    stratioStreamingMessage.getValue().execute();
                }
            }

        } catch (Exception e) {
            log.error("Error saving in Mongo: " + e.getMessage());
        }
    }

    @Override
    public Boolean check() throws Exception {
        try {
            getDB().getStats();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

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
}
