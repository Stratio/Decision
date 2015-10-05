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
package com.stratio.decision.service;

import com.stratio.decision.callbacks.ActionControllerCallback;
import com.stratio.decision.callbacks.StreamToActionBusCallback;
import com.stratio.decision.commons.constants.StreamAction;
import com.stratio.decision.commons.messages.StratioStreamingMessage;
import com.stratio.decision.serializer.Serializer;
import kafka.javaapi.producer.Producer;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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
