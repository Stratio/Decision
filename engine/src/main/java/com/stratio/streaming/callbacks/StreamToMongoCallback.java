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
package com.stratio.streaming.callbacks;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.event.in.InEvent;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import com.google.common.collect.Lists;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.stratio.streaming.commons.constants.STREAMING;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.utils.SiddhiUtils;

public class StreamToMongoCallback extends StreamCallback implements MessageListener<String> {

    private static Logger logger = LoggerFactory.getLogger(StreamToCassandraCallback.class);

    private StreamDefinition streamDefinition;
    private MongoClient mongoClient;
    private DB streamingDb;
    private Boolean running;

    public StreamToMongoCallback(StreamDefinition streamDefinition, String mongoHost, int mongoPort, String username,
            String password) throws UnknownHostException {
        this.streamDefinition = streamDefinition;
        running = Boolean.TRUE;
        List<ServerAddress> adresses = Arrays.asList(new ServerAddress(mongoHost, mongoPort));
        if (username != null && password != null) {
            mongoClient = new MongoClient(adresses, Arrays.asList(MongoCredential.createPlainCredential(username,
                    "$external", password.toCharArray())));
        } else {
            logger.warn(
                    "MongoDB user or password are not defined. User: {}, Password: {}. trying annonimous connection.",
                    username, password);
            mongoClient = new MongoClient(adresses);
        }
        streamingDb = mongoClient.getDB(STREAMING.STREAMING_KEYSPACE_NAME);
    }

    @Override
    public void receive(Event[] events) {

        if (running) {

            List<StratioStreamingMessage> collectedEvents = Lists.newArrayList();

            for (Event e : events) {

                if (e instanceof InEvent) {
                    InEvent ie = (InEvent) e;
                    List<ColumnNameTypeValue> columns = Lists.newArrayList();
                    for (Attribute column : streamDefinition.getAttributeList()) {
                        if (ie.getData().length >= streamDefinition.getAttributePosition(column.getName()) + 1) {
                            columns.add(new ColumnNameTypeValue(column.getName(), SiddhiUtils.encodeSiddhiType(column
                                    .getType()), ie.getData(streamDefinition.getAttributePosition(column.getName()))));
                        }
                    }
                    collectedEvents.add(new StratioStreamingMessage(streamDefinition.getStreamId(), ie.getTimeStamp(),
                            columns));
                }
            }
            persistEventsToMongo(collectedEvents);
        }

    }

    private void persistEventsToMongo(List<StratioStreamingMessage> collectedEvents) {
        long time = System.currentTimeMillis();
        Map<String, BulkWriteOperation> elementsToInsert = new HashMap<String, BulkWriteOperation>();
        for (StratioStreamingMessage event : collectedEvents) {
            BasicDBObject object = new BasicDBObject("timestamp", time);
            for (ColumnNameTypeValue columnNameTypeValue : event.getColumns()) {
                object.append(columnNameTypeValue.getColumn(), columnNameTypeValue.getValue());
            }

            BulkWriteOperation bulkInsertOperation = elementsToInsert.get(event.getStreamName());

            if (bulkInsertOperation == null) {
                bulkInsertOperation = streamingDb.getCollection(event.getStreamName())
                        .initializeUnorderedBulkOperation();

                elementsToInsert.put(event.getStreamName(), bulkInsertOperation);
                streamingDb.getCollection(event.getStreamName()).createIndex(new BasicDBObject("timestamp", -1));
            }
            bulkInsertOperation.insert(object);
        }
        for (Entry<String, BulkWriteOperation> stratioStreamingMessage : elementsToInsert.entrySet()) {
            stratioStreamingMessage.getValue().execute();
        }
    }

    private void shutdownCallback() {
        if (running) {
            mongoClient.close();
        }
    }

    @Override
    public void onMessage(Message<String> message) {
        if (running) {
            if (message.getMessageObject().equalsIgnoreCase(streamDefinition.getStreamId())
                    || message.getMessageObject().equalsIgnoreCase("*")) {
                shutdownCallback();
                running = Boolean.FALSE;
                logger.debug("Shutting down save2mongo for stream " + streamDefinition.getStreamId());
            }
        }

    }

}
