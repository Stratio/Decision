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
package com.stratio.streaming.utils;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiConfiguration;
import org.wso2.siddhi.query.api.QueryFactory;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.Attribute.Type;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.exception.AttributeNotExistException;
import org.wso2.siddhi.query.compiler.exception.SiddhiPraserException;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.stratio.streaming.commons.constants.ColumnType;
import com.stratio.streaming.commons.constants.STREAMING;
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.streams.Casandra2PersistenceStore;
import com.stratio.streaming.streams.StreamPersistence;

public class SiddhiUtils {

    private static Logger logger = LoggerFactory.getLogger(SiddhiUtils.class);

    public static final String QUERY_PLAN_IDENTIFIER = "StratioStreamingCEP-Cluster";

    private SiddhiUtils() {

    }

    public static Type decodeSiddhiType(ColumnType originalType) throws SiddhiPraserException {
        switch (originalType) {
        case STRING:
            return Attribute.Type.STRING;
        case BOOLEAN:
            return Attribute.Type.BOOL;
        case DOUBLE:
            return Attribute.Type.DOUBLE;
        case INTEGER:
            return Attribute.Type.INT;
        case LONG:
            return Attribute.Type.LONG;
        case FLOAT:
            return Attribute.Type.FLOAT;
        default:
            throw new SiddhiPraserException("Unsupported Column type: " + originalType);
        }
    }

    public static ColumnType encodeSiddhiType(Type type) throws SiddhiPraserException {
        switch (type) {
        case STRING:
            return ColumnType.STRING;
        case BOOL:
            return ColumnType.BOOLEAN;
        case DOUBLE:
            return ColumnType.DOUBLE;
        case INT:
            return ColumnType.INTEGER;
        case LONG:
            return ColumnType.LONG;
        case FLOAT:
            return ColumnType.FLOAT;
        default:
            throw new SiddhiPraserException("Unsupported Column type: " + type);
        }
    }

    public static String recoverStreamDefinition(StreamDefinition streamDefinition) {

        String attributesList = "";

        for (Attribute field : streamDefinition.getAttributeList()) {
            attributesList += field.getName() + " " + field.getType().toString().toLowerCase() + ",";
        }

        return "define stream " + streamDefinition.getStreamId() + "("
                + attributesList.substring(0, attributesList.length() - 1) + ")";
    }

    public static StreamDefinition buildDefineStreamSiddhiQL(StratioStreamingMessage request) {

        StreamDefinition newStream = QueryFactory.createStreamDefinition().name(request.getStreamName());

        for (ColumnNameTypeValue column : request.getColumns()) {
            logger.info(column.getColumn() + "//" + SiddhiUtils.decodeSiddhiType(column.getType()));
            try {
                newStream.attribute(column.getColumn(), SiddhiUtils.decodeSiddhiType(column.getType()));
            } catch (SiddhiPraserException e) {
                logger.info(e.getMessage() + "//" + column.getColumn() + "//" + column.getType());
            }
        }

        return newStream;
    }

    public static boolean columnAlreadyExistsInStream(String columnName, StreamDefinition streamMetaData) {

        for (Attribute column : streamMetaData.getAttributeList()) {
            if (column.getName().equalsIgnoreCase(columnName)) {
                return true;
            }
        }

        return false;
    }

    /**
     * 
     * - Instantiates the Siddhi CEP engine - return the running CEP engine
     * 
     * 
     * @return SiddhiManager
     */
    public static SiddhiManager setupSiddhiManager(String cassandraCluster, Boolean failOverEnabled) {

        SiddhiConfiguration conf = new SiddhiConfiguration();
        conf.setInstanceIdentifier("StratioStreamingCEP-Instance-" + UUID.randomUUID().toString());
        conf.setQueryPlanIdentifier(QUERY_PLAN_IDENTIFIER);
        conf.setDistributedProcessing(false);

        // Create Siddhi Manager
        SiddhiManager siddhiManager = new SiddhiManager(conf);

        Config config = new Config();
        config.setInstanceName("stratio-streaming-hazelcast-instance");
        NetworkConfig network = config.getNetworkConfig();
        JoinConfig join = network.getJoin();
        join.getMulticastConfig().setEnabled(false);

        siddhiManager.getSiddhiContext().setHazelcastInstance(HazelcastInstanceFactory.newHazelcastInstance(config));

        if (failOverEnabled) {

            siddhiManager.setPersistStore(new Casandra2PersistenceStore(cassandraCluster, "", ""));

            StreamPersistence.restoreLastRevision(siddhiManager);
        }

        return siddhiManager;
    }

    public static Object[] getOrderedValues(StreamDefinition streamMetaData, List<ColumnNameTypeValue> columns)
            throws AttributeNotExistException {

        Object[] orderedValues = new Object[streamMetaData.getAttributeList().size()];

        for (ColumnNameTypeValue column : columns) {

            // if attribute does not exist, a AttributeNotExistException
            // exception will be thrown
            // TODO change this. create a conversor engine to treat data types.
            if (column.getValue() instanceof String) {
                orderedValues[streamMetaData.getAttributePosition(column.getColumn())] = decodeSiddhiValue(
                        (String) column.getValue(), streamMetaData.getAttributeType(column.getColumn()));
            } else if (column.getValue() instanceof Double) {
                orderedValues[streamMetaData.getAttributePosition(column.getColumn())] = decodeSiddhiValue(
                        (Double) column.getValue(), streamMetaData.getAttributeType(column.getColumn()));
            } else {
                orderedValues[streamMetaData.getAttributePosition(column.getColumn())] = column.getValue();
            }

        }

        return orderedValues;

    }

    private static Object decodeSiddhiValue(String originalValue, Attribute.Type type) throws SiddhiPraserException {

        switch (type) {
        case STRING:
            return originalValue;
        case BOOL:
            return Boolean.valueOf(originalValue);
        case DOUBLE:
            return Double.valueOf(originalValue);
        case INT:
            return Integer.valueOf(originalValue);
        case LONG:
            return Long.valueOf(originalValue);
        case FLOAT:
            return Float.valueOf(originalValue);
        default:
            throw new SiddhiPraserException("Unsupported Column type: " + originalValue + "/" + type.toString());
        }

    }

    private static Object decodeSiddhiValue(Double originalValue, Attribute.Type type) throws SiddhiPraserException {

        switch (type) {
        case STRING:
            return String.valueOf(originalValue);
        case DOUBLE:
            return Double.valueOf(originalValue);
        case INT:
            return originalValue.intValue();
        case LONG:
            return originalValue.longValue();
        case FLOAT:
            return originalValue.floatValue();
        default:
            throw new SiddhiPraserException("Unsupported Column type: " + originalValue + "/" + type.toString());
        }

    }

    // TODO move to StreamingCommons
    public static Boolean isStreamAllowedForThisOperation(String streamName, String operation) {

        switch (operation.toUpperCase()) {
        case STREAM_OPERATIONS.DEFINITION.ADD_QUERY:
        case STREAM_OPERATIONS.DEFINITION.ALTER:
        case STREAM_OPERATIONS.DEFINITION.CREATE:
        case STREAM_OPERATIONS.DEFINITION.DROP:
        case STREAM_OPERATIONS.DEFINITION.REMOVE_QUERY:
        case STREAM_OPERATIONS.MANIPULATION.INSERT:
            if (Arrays.asList(STREAMING.STATS_NAMES.STATS_STREAMS).contains(streamName)) {
                return Boolean.FALSE;
            }
            return Boolean.TRUE;

        case STREAM_OPERATIONS.ACTION.LISTEN:
        case STREAM_OPERATIONS.ACTION.SAVETO_CASSANDRA:
        case STREAM_OPERATIONS.ACTION.SAVETO_MONGO:
        case STREAM_OPERATIONS.ACTION.STOP_SAVETO_MONGO:
        case STREAM_OPERATIONS.ACTION.STOP_SAVETO_CASSANDRA:
        case STREAM_OPERATIONS.ACTION.INDEX:
        case STREAM_OPERATIONS.ACTION.STOP_LISTEN:
        case STREAM_OPERATIONS.ACTION.STOP_INDEX:
        case STREAM_OPERATIONS.MANIPULATION.LIST:

            return Boolean.TRUE;
        default:
            return Boolean.FALSE;
        }
    }

}
