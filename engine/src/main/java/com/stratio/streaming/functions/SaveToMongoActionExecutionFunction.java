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
package com.stratio.streaming.functions;

import com.mongodb.*;
import com.stratio.streaming.commons.constants.STREAMING;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
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

    public SaveToMongoActionExecutionFunction(List<String> mongoHosts, String username, String password) {
        this.mongoHosts = mongoHosts;
        this.username = username;
        this.password = password;
    }

    @Override
    public void process(Iterable<StratioStreamingMessage> messages) throws Exception {
        Map<String, BulkWriteOperation> elementsToInsert = new HashMap<String, BulkWriteOperation>();

        for (StratioStreamingMessage event : messages) {
            BasicDBObject object = new BasicDBObject(TIMESTAMP_FIELD, event.getTimestamp());
            for (ColumnNameTypeValue columnNameTypeValue : event.getColumns()) {
                object.append(columnNameTypeValue.getColumn(), columnNameTypeValue.getValue());
            }

            BulkWriteOperation bulkInsertOperation = elementsToInsert.get(event.getStreamName());

            if (bulkInsertOperation == null) {
                bulkInsertOperation = getDB().getCollection(event.getStreamName()).initializeUnorderedBulkOperation();

                elementsToInsert.put(event.getStreamName(), bulkInsertOperation);
                getDB().getCollection(event.getStreamName()).createIndex(new BasicDBObject(TIMESTAMP_FIELD, -1));
            }

            bulkInsertOperation.insert(object);
        }

        for (Entry<String, BulkWriteOperation> stratioStreamingMessage : elementsToInsert.entrySet()) {
            stratioStreamingMessage.getValue().execute();
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
