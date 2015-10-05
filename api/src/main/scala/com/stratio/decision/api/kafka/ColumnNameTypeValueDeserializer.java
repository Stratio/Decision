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
package com.stratio.decision.api.kafka;

import com.google.gson.*;
import com.stratio.decision.commons.constants.ColumnType;
import com.stratio.decision.commons.messages.ColumnNameTypeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;

public class ColumnNameTypeValueDeserializer implements JsonDeserializer<ColumnNameTypeValue> {

    private static Logger log = LoggerFactory.getLogger(ColumnNameTypeValueDeserializer.class);

    private static final String COLUMN_FIELD = "column";
    private static final String TYPE_FIELD = "type";
    private static final String VALUE_FIELD = "value";

    @Override
    public ColumnNameTypeValue deserialize(JsonElement element, Type type, JsonDeserializationContext ctx)
            throws JsonParseException {
        final JsonObject object = element.getAsJsonObject();
        String name = null;
        ColumnType columnType = null;
        Object value = null;
        if (object != null && object.has(COLUMN_FIELD) && object.has(TYPE_FIELD)) {
            name = object.get(COLUMN_FIELD).getAsString();
            columnType = ColumnType.valueOf(object.get(TYPE_FIELD).getAsString());

            if (object.has(VALUE_FIELD)) {
                JsonElement jsonValue = object.get(VALUE_FIELD);
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
            } else {
                log.warn("Column with name {} has no value", name);
            }

            if (log.isDebugEnabled()) {
                log.debug("Values obtained into ColumnNameTypeValue deserialization: " +
                        "NAME: {}, VALUE: {}, COLUMNTYPE: {}", name, value, columnType);
            }
        } else {
            log.warn("Error deserializing ColumnNameTypeValue from json. JsonObject is not complete: {}", element);
        }

        return new ColumnNameTypeValue(name, columnType, value);
    }
}
