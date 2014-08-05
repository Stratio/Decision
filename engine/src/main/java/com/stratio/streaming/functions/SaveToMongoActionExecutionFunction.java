package com.stratio.streaming.functions;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.stratio.streaming.commons.constants.STREAMING;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;

public class SaveToMongoActionExecutionFunction extends BaseActionExecutionFunction {

    private static final long serialVersionUID = -7890478075965866737L;

    private static Logger log = LoggerFactory.getLogger(SaveToMongoActionExecutionFunction.class);

    private MongoClient mongoClient;
    private DB streamingDb;

    private final String mongoHost;
    private final int mongoPort;
    private final String username;
    private final String password;

    public SaveToMongoActionExecutionFunction(String mongoHost, int mongoPort, String username, String password) {
        this.mongoHost = mongoHost;
        this.mongoPort = mongoPort;
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
            List<ServerAddress> adresses = Arrays.asList(new ServerAddress(mongoHost, mongoPort));
            if (username != null && password != null) {
                mongoClient = new MongoClient(adresses, Arrays.asList(MongoCredential.createPlainCredential(username,
                        "$external", password.toCharArray())));
            } else {
                log.warn(
                        "MongoDB user or password are not defined. User: [{}], Password: [{}]. trying annonimous connection.",
                        username, password);
                mongoClient = new MongoClient(adresses);
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
