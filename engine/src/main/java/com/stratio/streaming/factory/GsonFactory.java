package com.stratio.streaming.factory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.serializer.gson.impl.ColumnNameTypeValueDeserializer;

public class GsonFactory {

    private static Gson INSTANCE;

    public static Gson getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new GsonBuilder().registerTypeAdapter(ColumnNameTypeValue.class,
                    new ColumnNameTypeValueDeserializer()).create();
        }
        return INSTANCE;
    }
}
