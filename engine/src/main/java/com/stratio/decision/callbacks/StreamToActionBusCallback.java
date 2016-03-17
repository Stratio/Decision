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
package com.stratio.decision.callbacks;

import com.stratio.decision.commons.constants.InternalTopic;
import com.stratio.decision.commons.constants.StreamAction;
import com.stratio.decision.commons.messages.StratioStreamingMessage;
import com.stratio.decision.serializer.Serializer;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.event.Event;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class StreamToActionBusCallback extends ActionControllerCallback {

    private static final Logger log = LoggerFactory.getLogger(StreamToActionBusCallback.class);

    private final String streamName;

    private final Producer<String, byte[]> avroProducer;

    private final Serializer<StratioStreamingMessage, Event> javaToSiddhiSerializer;
    private final Serializer<StratioStreamingMessage, byte[]> javaToAvroSerializer;

    private String groupId;

    public StreamToActionBusCallback(Set<StreamAction> activeActions, String streamName,
            Producer<String, byte[]> avroProducer,
            Serializer<StratioStreamingMessage, Event> javaToSiddhiSerializer,
            Serializer<StratioStreamingMessage, byte[]> javaToAvroSerializer) {
        super(activeActions);
        this.streamName = streamName;
        this.avroProducer = avroProducer;
        this.javaToSiddhiSerializer = javaToSiddhiSerializer;
        this.javaToAvroSerializer = javaToAvroSerializer;
    }

    public StreamToActionBusCallback(Set<StreamAction> activeActions, String streamName,
            Producer<String, byte[]> avroProducer,
            Serializer<StratioStreamingMessage, Event> javaToSiddhiSerializer,
            Serializer<StratioStreamingMessage, byte[]> javaToAvroSerializer,
            String groupId) {

        this(activeActions, streamName, avroProducer, javaToSiddhiSerializer,
                javaToAvroSerializer);
        this.groupId = groupId;
    }

    @Override
    public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
        if (log.isDebugEnabled()) {
            log.debug("Receiving {} events from stream {}", inEvents.length, streamName);
        }

        String topicAction = InternalTopic.TOPIC_ACTION.getTopicName();
        if (groupId !=null){
            topicAction = topicAction.concat("_").concat(groupId);
        }

        List<KeyedMessage<String, byte[]>> messages = new ArrayList<>();

        for (Event event : inEvents) {
            StratioStreamingMessage messageObject = javaToSiddhiSerializer.deserialize(event);

            messageObject.setStreamName(streamName);
            messageObject.setActiveActions(this.activeActions);

            messages.add(new KeyedMessage<String, byte[]>(topicAction,
                    javaToAvroSerializer.serialize(messageObject)));

        }

        if (messages.size() != 0) {
            avroProducer.send(messages);
        }
    }
}
