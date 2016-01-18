package com.stratio.decision.functions.engine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

    protected  Producer<String, String> producer;
    protected  Serializer<String, StratioStreamingMessage> kafkaToJavaSerializer;
    protected  Serializer<StratioStreamingMessage, Event> javaToSiddhiSerializer;
    protected  SiddhiManager siddhiManager;

    private Map<String, Object> engineParameters;

    private StreamOperationServiceWithoutMetrics streamOperationService;

//    public BaseEngineAction(Object[] engineParameters){
//
//        this.engineParameters = engineParameters;
//    }
//
//    public BaseEngineAction(Object[] engineParameters, Producer<String, String> producer, Serializer<String,
//            StratioStreamingMessage> kafkaToJavaSerializer,
//            Serializer<StratioStreamingMessage, Event> javaToSiddhiSerializer, SiddhiManager siddhiManager) {
//
//        this.producer = producer;
//        this.kafkaToJavaSerializer = kafkaToJavaSerializer;
//        this.javaToSiddhiSerializer = javaToSiddhiSerializer;
//        this.siddhiManager = siddhiManager;
//        this.engineParameters = engineParameters;
//
//    }
//
//    public BaseEngineAction(Object[] engineParameters, SiddhiManager siddhiManager) {
//
//        this.siddhiManager = siddhiManager;
//        this.engineParameters = engineParameters;
//
//    }

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

    private void insertColumnsIntoStream(String streamName, List<ColumnNameTypeValue> columns) {

       List<ColumnNameTypeValue> notCreatedColumns = columns.stream().filter (
               column -> !streamOperationService.columnExists(streamName, column.getColumn())
       ).collect(Collectors.toList());

        try {
            streamOperationService.enlargeStream(streamName, notCreatedColumns, false);
            streamOperationService.send(streamName, columns);
        }
        catch (Exception e) {}// TODO

    }

    protected void handleCepRedirection(String streamName, List<Map<String, Object>> formattedResults){

        if (!streamOperationService.streamExist(streamName)){
            streamOperationService.createStream(streamName, null);
        }

        formattedResults.stream().forEach(

                result -> {

                    List<ColumnNameTypeValue> columns = new ArrayList<>();

                    result.forEach(

                            (fieldName, fieldValue) -> {

                                if (!fieldName.equals("class")){
                                    ColumnNameTypeValue column = getColumnNameTypeValue(fieldName, fieldValue);
                                    if (column!=null) {
                                        columns.add(column);
                                    }
                                }
                            }
                    );
                    // insert values
                    insertColumnsIntoStream(streamName, columns);
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
