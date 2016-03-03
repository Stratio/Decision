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
package com.stratio.decision.functions.engine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;

import com.stratio.decision.commons.constants.ColumnType;
import com.stratio.decision.commons.messages.ColumnNameTypeValue;
import com.stratio.decision.commons.messages.StratioStreamingMessage;
import com.stratio.decision.serializer.Serializer;
import com.stratio.decision.service.StreamOperationServiceWithoutMetrics;

import kafka.javaapi.producer.Producer;

/**
 * Created by josepablofernandez on 2/12/15.
 */
public abstract class BaseEngineAction  implements EngineAction {

    private static final Logger logger = LoggerFactory.getLogger(BaseEngineAction.class);

    protected  Producer<String, String> producer;
    protected  Serializer<String, StratioStreamingMessage> kafkaToJavaSerializer;
    protected  Serializer<StratioStreamingMessage, Event> javaToSiddhiSerializer;
    protected  SiddhiManager siddhiManager;

    private Map<String, Object> engineParameters;

    private StreamOperationServiceWithoutMetrics streamOperationService;

    public BaseEngineAction(Map<String, Object> engineParameters, SiddhiManager siddhiManager,
            StreamOperationServiceWithoutMetrics streamOperationService) {

        this.siddhiManager = siddhiManager;
        this.engineParameters = engineParameters;
        this.streamOperationService = streamOperationService;

    }

    @Override
    abstract public void execute(String streamName, Event[] events);


    private ColumnType getColumnType(Object object){


        if (object instanceof String){
            return ColumnType.STRING;
        } else if (object instanceof Boolean) {
            return ColumnType.BOOLEAN;
        } else if (object instanceof Double) {
            return ColumnType.DOUBLE;
        } else if (object instanceof Integer) {
            return ColumnType.INTEGER;
        } else if (object instanceof Long) {
            return ColumnType.LONG;
        } else if (object instanceof Float) {
            return ColumnType.FLOAT;
        }

        return null;
    }

    private ColumnNameTypeValue getColumnNameTypeValue(String column, Object value) {

        ColumnType type = getColumnType(value);
        if (type!=null) {
            return new ColumnNameTypeValue(column, type, value);
        }

        return null;
    }

    private void insertColumnsIntoStream(String streamName, List<ColumnNameTypeValue> columns) throws Exception {

       List<ColumnNameTypeValue> notCreatedColumns = columns.stream().filter (
               column -> !streamOperationService.columnExists(streamName, column.getColumn())
       ).collect(Collectors.toList());

       streamOperationService.enlargeStream(streamName, notCreatedColumns, false);
       streamOperationService.send(streamName, columns);

    }

    protected void handleCepRedirection(String streamName, List<Map<String, Object>> formattedResults){


            if (!streamOperationService.streamExist(streamName)) {
                streamOperationService.createStream(streamName, null);
            }

            formattedResults.stream().forEach(

                    result -> {

                        try {
                            List<ColumnNameTypeValue> columns = new ArrayList<>();

                            result.forEach(

                                    (fieldName, fieldValue) -> {

                                        if (!fieldName.equals("class")) {
                                            ColumnNameTypeValue column = getColumnNameTypeValue(fieldName, fieldValue);
                                            if (column != null) {
                                                columns.add(column);
                                            }
                                        }
                                    }
                            );
                            // insert values
                            insertColumnsIntoStream(streamName, columns);
                        }catch(Exception e){

                            logger.error("Error handle cep redirection for stream name {}. {} ", streamName, e.getMessage());
                        }
                    }
            );
    }

    public Map<String, Object> getEngineParameters() {
        return engineParameters;
    }

    public void setEngineParameters(Map<String, Object> engineParameters) {
        this.engineParameters = engineParameters;
    }

    public Producer<String, String> getProducer() {
        return producer;
    }

    public void setProducer(Producer<String, String> producer) {
        this.producer = producer;
    }

    public Serializer<String, StratioStreamingMessage> getKafkaToJavaSerializer() {
        return kafkaToJavaSerializer;
    }

    public void setKafkaToJavaSerializer(
            Serializer<String, StratioStreamingMessage> kafkaToJavaSerializer) {
        this.kafkaToJavaSerializer = kafkaToJavaSerializer;
    }

    public Serializer<StratioStreamingMessage, Event> getJavaToSiddhiSerializer() {
        return javaToSiddhiSerializer;
    }

    public void setJavaToSiddhiSerializer(
            Serializer<StratioStreamingMessage, Event> javaToSiddhiSerializer) {
        this.javaToSiddhiSerializer = javaToSiddhiSerializer;
    }

    public SiddhiManager getSiddhiManager() {
        return siddhiManager;
    }

    public void setSiddhiManager(SiddhiManager siddhiManager) {
        this.siddhiManager = siddhiManager;
    }

    public StreamOperationServiceWithoutMetrics getStreamOperationService() {
        return streamOperationService;
    }

    public void setStreamOperationService(
            StreamOperationServiceWithoutMetrics streamOperationService) {
        this.streamOperationService = streamOperationService;
    }
}
