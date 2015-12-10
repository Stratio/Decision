package com.stratio.decision.functions.engine;

import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;

import com.stratio.decision.commons.messages.StratioStreamingMessage;
import com.stratio.decision.serializer.Serializer;

import kafka.javaapi.producer.Producer;

/**
 * Created by josepablofernandez on 2/12/15.
 */
public abstract class BaseEngineAction  implements EngineAction {

    protected  Producer<String, String> producer;
    protected  Serializer<String, StratioStreamingMessage> kafkaToJavaSerializer;
    protected  Serializer<StratioStreamingMessage, Event> javaToSiddhiSerializer;
    protected  SiddhiManager siddhiManager;

    private Object[] engineParameters;

    public BaseEngineAction(Object[] engineParameters){

        this.engineParameters = engineParameters;
    }

    public BaseEngineAction(Object[] engineParameters, Producer<String, String> producer, Serializer<String,
            StratioStreamingMessage> kafkaToJavaSerializer,
            Serializer<StratioStreamingMessage, Event> javaToSiddhiSerializer, SiddhiManager siddhiManager) {

        this.producer = producer;
        this.kafkaToJavaSerializer = kafkaToJavaSerializer;
        this.javaToSiddhiSerializer = javaToSiddhiSerializer;
        this.siddhiManager = siddhiManager;
        this.engineParameters = engineParameters;

    }

    public BaseEngineAction(Object[] engineParameters, SiddhiManager siddhiManager) {

        this.siddhiManager = siddhiManager;
        this.engineParameters = engineParameters;

    }

    @Override
    abstract public void execute(String streamName, Event[] events);

    public Object[] getEngineParameters() {
        return engineParameters;
    }

    public void setEngineParameters(Object[] engineParameters) {
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
}
