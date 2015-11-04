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

import java.util.ArrayList;
import java.util.List;

import com.stratio.decision.commons.messages.StratioStreamingMessage;

import kafka.producer.KeyedMessage;

public class SendToKafkaActionExecutionFunction extends KafkaBaseExecutionFunction {

    private static final long serialVersionUID = -1661238643911306344L;

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

}
