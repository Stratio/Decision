package com.stratio.streaming.serializer.impl;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.serializer.Serializer;

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
