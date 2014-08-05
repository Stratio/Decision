package com.stratio.streaming.serializer.gson.impl;

import java.lang.reflect.Type;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.stratio.streaming.commons.constants.ColumnType;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;

public class ColumnNameTypeValueDeserializer implements JsonDeserializer<ColumnNameTypeValue> {

    @Override
    public ColumnNameTypeValue deserialize(JsonElement element, Type type, JsonDeserializationContext ctx)
            throws JsonParseException {
        final JsonObject object = element.getAsJsonObject();

        String name = object.get("column").getAsString();
        ColumnType columnType = ColumnType.valueOf(object.get("type").getAsString());
        JsonElement jsonValue = object.get("value");
        Object value = null;

        switch (columnType) {
        case BOOLEAN:
            value = jsonValue.getAsBoolean();
            break;
        case DOUBLE:
            value = jsonValue.getAsDouble();
            break;
        case FLOAT:
            value = jsonValue.getAsFloat();
            break;
        case INTEGER:
            value = jsonValue.getAsInt();
            break;
        case LONG:
            value = jsonValue.getAsLong();
            break;
        case STRING:
            value = jsonValue.getAsString();
            break;
        default:
            break;
        }

        return new ColumnNameTypeValue(name, columnType, value);
    }
}
