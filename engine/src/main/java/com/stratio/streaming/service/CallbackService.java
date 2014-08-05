package com.stratio.streaming.service;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import kafka.javaapi.producer.Producer;

import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;

import com.stratio.streaming.callbacks.ActionControllerCallback;
import com.stratio.streaming.callbacks.StreamToActionBusCallback;
import com.stratio.streaming.commons.constants.StreamAction;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.serializer.Serializer;

public class CallbackService {

    private final Producer<String, String> producer;
    private final Serializer<String, StratioStreamingMessage> kafkaToJavaSerializer;
    private final Serializer<StratioStreamingMessage, Event> javaToSiddhiSerializer;

    private final Map<String, ActionControllerCallback> referencedCallbacks;

    public CallbackService(Producer<String, String> producer,
            Serializer<String, StratioStreamingMessage> kafkaToJavaSerializer,
            Serializer<StratioStreamingMessage, Event> javaToSiddhiSerializer) {
        this.producer = producer;
        this.kafkaToJavaSerializer = kafkaToJavaSerializer;
        this.javaToSiddhiSerializer = javaToSiddhiSerializer;
        this.referencedCallbacks = new HashMap<>();
    }

    public QueryCallback add(String streamName, Set<StreamAction> actions) {

        ActionControllerCallback callback = referencedCallbacks.get(streamName);

        if (callback == null) {
            callback = new StreamToActionBusCallback(actions, streamName, producer, kafkaToJavaSerializer,
                    javaToSiddhiSerializer);
            referencedCallbacks.put(streamName, callback);
        }

        return callback;
    }

    public void remove(String streamName) {
        referencedCallbacks.remove(streamName);
    }
}
