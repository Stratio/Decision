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

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.factory.GsonFactory;
import com.stratio.streaming.serializer.impl.KafkaToJavaSerializer;

public class SerializerFunction implements Function<Tuple2<String, String>, StratioStreamingMessage> {

    private static final long serialVersionUID = 1150382218319606622L;

    private KafkaToJavaSerializer kafkaToJavaSerializer;

    @Override
    public StratioStreamingMessage call(Tuple2<String, String> object) throws Exception {
        return getKafkaToJavaSerializer().serialize(object._2());
    }

    private KafkaToJavaSerializer getKafkaToJavaSerializer() {
        if (kafkaToJavaSerializer == null) {
            kafkaToJavaSerializer = new KafkaToJavaSerializer(GsonFactory.getInstance());
        }
        return kafkaToJavaSerializer;
    }
}
