package com.stratio.streaming.callbacks;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.event.Event;

import com.stratio.streaming.commons.constants.InternalTopic;
import com.stratio.streaming.commons.constants.StreamAction;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.serializer.Serializer;

public class StreamToActionBusCallback extends ActionControllerCallback {

    private static final Logger log = LoggerFactory.getLogger(StreamToActionBusCallback.class);

    private final String streamName;

    private final Producer<String, String> producer;

    private final Serializer<String, StratioStreamingMessage> kafkaToJavaSerializer;
    private final Serializer<StratioStreamingMessage, Event> javaToSiddhiSerializer;

    public StreamToActionBusCallback(Set<StreamAction> activeActions, String streamName,
            Producer<String, String> producer, Serializer<String, StratioStreamingMessage> kafkaToJavaSerializer,
            Serializer<StratioStreamingMessage, Event> javaToSiddhiSerializer) {
        super(activeActions);
        this.streamName = streamName;
        this.producer = producer;
        this.kafkaToJavaSerializer = kafkaToJavaSerializer;
        this.javaToSiddhiSerializer = javaToSiddhiSerializer;
    }

    @Override
    public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
        if (log.isDebugEnabled()) {
            log.debug("Receiving {} events from stream {}", inEvents.length, streamName);
        }
        List<KeyedMessage<String, String>> messages = new ArrayList<>();
        for (Event event : inEvents) {
            StratioStreamingMessage messageObject = javaToSiddhiSerializer.deserialize(event);

            messageObject.setStreamName(streamName);
            messageObject.setActiveActions(this.activeActions);

            messages.add(new KeyedMessage<String, String>(InternalTopic.TOPIC_ACTION.getTopicName(),
                    kafkaToJavaSerializer.deserialize(messageObject)));
        }

        if (messages.size() != 0) {
            producer.send(messages);
        }
    }
}
