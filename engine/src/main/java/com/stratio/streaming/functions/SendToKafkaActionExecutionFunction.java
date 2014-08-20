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
package com.stratio.streaming.functions;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.factory.GsonFactory;
import com.stratio.streaming.serializer.Serializer;
import com.stratio.streaming.serializer.impl.KafkaToJavaSerializer;

public class SendToKafkaActionExecutionFunction extends BaseActionExecutionFunction {

    private static final long serialVersionUID = -1661238643911306344L;

    private Producer<String, String> producer;
    private KafkaToJavaSerializer kafkaToJavaSerializer;

    private final String kafkaQuorum;

    public SendToKafkaActionExecutionFunction(String kafkaQuorum) {
        this.kafkaQuorum = kafkaQuorum;
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

    private Serializer<String, StratioStreamingMessage> getSerializer() {
        if (kafkaToJavaSerializer == null) {
            kafkaToJavaSerializer = new KafkaToJavaSerializer(GsonFactory.getInstance());
        }
        return kafkaToJavaSerializer;
    }

    private Producer<String, String> getProducer() {
        if (producer == null) {
            Properties properties = new Properties();
            properties.put("serializer.class", "kafka.serializer.StringEncoder");
            properties.put("metadata.broker.list", kafkaQuorum);
            properties.put("producer.type", "async");
            producer = new Producer<String, String>(new ProducerConfig(properties));
        }
        return producer;
    }
}
