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
package com.stratio.decision.utils;

import com.stratio.decision.commons.constants.STREAMING;
import com.stratio.decision.commons.constants.STREAM_OPERATIONS;
import com.stratio.decision.commons.messages.ColumnNameTypeValue;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.exception.AttributeNotExistException;

import java.util.Arrays;
import java.util.List;

public class SiddhiUtils {

    public static final String QUERY_PLAN_IDENTIFIER = "StratioStreamingCEP-Cluster";

    private SiddhiUtils() {

    }

    public static boolean columnAlreadyExistsInStream(String columnName, StreamDefinition streamMetaData) {

        for (Attribute column : streamMetaData.getAttributeList()) {
            if (column.getName().equalsIgnoreCase(columnName)) {
                return true;
            }
        }

        return false;
    }

    public static Object[] getOrderedValues(StreamDefinition streamMetaData, List<ColumnNameTypeValue> columns)
            throws AttributeNotExistException {

        Object[] orderedValues = new Object[streamMetaData.getAttributeList().size()];

        for (ColumnNameTypeValue column : columns) {

            // if attribute does not exist, a AttributeNotExistException
            // exception will be thrown
            // XXX change this. create a conversor engine to treat data types.
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

    protected static Object decodeSiddhiValue(String originalValue, Attribute.Type type) {

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
            throw new RuntimeException("Unsupported Column type: " + originalValue + "/" + type.toString());
        }

    }

    protected static Object decodeSiddhiValue(Double originalValue, Attribute.Type type) {

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
            throw new RuntimeException("Unsupported Column type: " + originalValue + "/" + type.toString());
        }

    }

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
        case STREAM_OPERATIONS.ACTION.SAVETO_SOLR:
        case STREAM_OPERATIONS.ACTION.START_SENDTODROOLS:
        case STREAM_OPERATIONS.ACTION.STOP_SAVETO_MONGO:
        case STREAM_OPERATIONS.ACTION.STOP_SAVETO_CASSANDRA:
        case STREAM_OPERATIONS.ACTION.STOP_SAVETO_SOLR:
        case STREAM_OPERATIONS.ACTION.STOP_SENDTODROOLS:
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
