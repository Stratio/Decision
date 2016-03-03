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

import java.util.Properties;

import com.stratio.decision.commons.messages.StratioStreamingMessage;
import com.stratio.decision.factory.GsonFactory;
import com.stratio.decision.serializer.Serializer;
import com.stratio.decision.serializer.impl.KafkaToJavaSerializer;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

/**
 * Created by aitor on 4/11/15.
 */
public abstract class KafkaBaseExecutionFunction extends BaseActionExecutionFunction {

    protected Producer<String, String> producer;
    protected KafkaToJavaSerializer kafkaToJavaSerializer;

    protected String kafkaQuorum;


    protected Serializer<String, StratioStreamingMessage> getSerializer() {
        if (kafkaToJavaSerializer == null) {
            kafkaToJavaSerializer = new KafkaToJavaSerializer(GsonFactory.getInstance());
        }
        return kafkaToJavaSerializer;
    }

    protected Producer<String, String> getProducer() {
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
