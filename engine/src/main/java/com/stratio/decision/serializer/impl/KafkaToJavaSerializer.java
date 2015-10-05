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
package com.stratio.decision.serializer.impl;

import com.google.gson.Gson;
import com.stratio.decision.commons.messages.StratioStreamingMessage;
import com.stratio.decision.serializer.Serializer;

import java.util.ArrayList;
import java.util.List;

public class KafkaToJavaSerializer implements Serializer<String, StratioStreamingMessage> {

    private static final long serialVersionUID = -8718476581709868690L;

    private final Gson gson;

    public KafkaToJavaSerializer(Gson gson) {
        this.gson = gson;
    }

    @Override
    public StratioStreamingMessage serialize(String object) {
        return gson.fromJson(object, StratioStreamingMessage.class);
    }

    @Override
    public String deserialize(StratioStreamingMessage object) {
        return gson.toJson(object);
    }

    @Override
    public List<StratioStreamingMessage> serialize(List<String> object) {
        List<StratioStreamingMessage> result = new ArrayList<>();
        if (object != null) {
            for (String string : object) {
                result.add(serialize(string));
            }
        }

        return result;
    }

    @Override
    public List<String> deserialize(List<StratioStreamingMessage> object) {
        List<String> result = new ArrayList<>();
        if (object != null) {
            for (StratioStreamingMessage message : object) {
                result.add(deserialize(message));
            }
        }
        return result;
    }

}
