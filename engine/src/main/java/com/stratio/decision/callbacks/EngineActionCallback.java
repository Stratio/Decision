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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.event.StreamEvent;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;

import com.stratio.decision.commons.messages.StratioStreamingMessage;
import com.stratio.decision.functions.engine.BaseEngineAction;
import com.stratio.decision.serializer.Serializer;

import kafka.javaapi.producer.Producer;

/**
 * Created by josepablofernandez on 2/12/15.
 */
public class EngineActionCallback extends QueryCallback {

    private static final Logger log = LoggerFactory.getLogger(EngineActionCallback.class);

    private final String streamName;

    private final Producer<String, String> producer;

    private final Serializer<String, StratioStreamingMessage> kafkaToJavaSerializer;
    private final Serializer<StratioStreamingMessage, Event> javaToSiddhiSerializer;

    private BaseEngineAction engineAction;

    public EngineActionCallback(String streamName, BaseEngineAction engineAction, Producer<String, String> producer,
            Serializer<String,  StratioStreamingMessage> kafkaToJavaSerializer,
            Serializer<StratioStreamingMessage, Event> javaToSiddhiSerializer) {

        super();
        this.streamName = streamName;
        this.engineAction = engineAction;
        this.producer = producer;
        this.kafkaToJavaSerializer = kafkaToJavaSerializer;
        this.javaToSiddhiSerializer = javaToSiddhiSerializer;

    }

    @Override public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {

        if (log.isDebugEnabled()) {

            log.debug("Receiving {} events from stream {}", inEvents.length, streamName);
        }

        engineAction.setProducer(this.producer);
        engineAction.setJavaToSiddhiSerializer(this.javaToSiddhiSerializer);
        engineAction.setKafkaToJavaSerializer(this.kafkaToJavaSerializer);

        engineAction.execute(streamName, inEvents);

        if (log.isDebugEnabled()) {

            log.debug("Finished EngineAction execution from stream {}",  streamName);
        }

    }
}
