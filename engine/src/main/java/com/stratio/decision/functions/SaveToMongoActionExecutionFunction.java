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
package com.stratio.decision.functions;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.stratio.decision.commons.messages.ColumnNameTypeValue;
import com.stratio.decision.commons.messages.StratioStreamingMessage;

public class SaveToMongoActionExecutionFunction extends BaseActionExecutionFunction {

    private static final long serialVersionUID = -7890478075965866737L;

    private static Logger log = LoggerFactory.getLogger(SaveToMongoActionExecutionFunction.class);

    private transient MongoClient mongoClient;
    private transient DB streamingDb;

    private final List<String> mongoHosts;
    private final String username;
    private final String password;
    private final Integer maxBatchSize;


    public SaveToMongoActionExecutionFunction(List<String> mongoHosts, String username, String password, Integer
            maxBatchSize, MongoClient mongoClient, DB streamingDb) {

        this(mongoHosts, username, password, maxBatchSize);
        this.mongoClient = mongoClient;
        this.streamingDb = streamingDb;
    }

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

        if (partitionSize == null || partitionSize <= 0){
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

        if (mongoClient == null){
            mongoClient = (MongoClient) ActionBaseContext.getInstance().getContext().getBean
                    ("mongoClient");
        }

        return mongoClient;
    }

    private DB getDB() throws UnknownHostException {
        if (streamingDb == null) {
            streamingDb= (DB) ActionBaseContext.getInstance().getContext().getBean
                    ("mongoDB");
        }
        return streamingDb;
    }
}
