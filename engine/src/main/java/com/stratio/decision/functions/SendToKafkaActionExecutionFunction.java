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
package com.stratio.decision.functions;

import com.stratio.decision.commons.messages.StratioStreamingMessage;
import com.stratio.decision.factory.GsonFactory;
import com.stratio.decision.serializer.Serializer;
import com.stratio.decision.serializer.impl.KafkaToJavaSerializer;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class SendToKafkaActionExecutionFunction extends BaseActionExecutionFunction {

    private static final long serialVersionUID = -1661238643911306344L;

    private transient Producer<String, String> producer;
    private KafkaToJavaSerializer kafkaToJavaSerializer;

    private final String kafkaQuorum;

    public SendToKafkaActionExecutionFunction(String kafkaQuorum) {
        this.kafkaQuorum = kafkaQuorum;
    }

    public SendToKafkaActionExecutionFunction(String kafkaQuorum, Producer<String, String> producer) {
        this(kafkaQuorum);
        this.producer = producer;
    }

    @Override
    public void process(Iterable<StratioStreamingMessage> messages) throws Exception {
        List<KeyedMessage<String, String>> kafkaMessages = new ArrayList<>();
        for (StratioStreamingMessage message : messages) {
            kafkaMessages.add(new KeyedMessage<String, String>(message.getStreamName(), getSerializer().deserialize(
                    message)));
        }
        getProducer().send(kafkaMessages);
    }

    @Override
    public Boolean check() throws Exception {
        return null;
    }

    private Serializer<String, StratioStreamingMessage> getSerializer() {
        if (kafkaToJavaSerializer == null) {
            kafkaToJavaSerializer = new KafkaToJavaSerializer(GsonFactory.getInstance());
        }
        return kafkaToJavaSerializer;
    }

    private Producer<String, String> getProducer() {
        if (producer == null) {

            producer = (Producer) ActionBaseContext.getInstance().getContext().getBean
                    ("producer");
        }
        return producer;
    }

}