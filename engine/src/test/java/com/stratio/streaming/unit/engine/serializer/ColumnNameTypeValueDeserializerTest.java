package com.stratio.streaming.unit.engine.serializer;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.serializer.gson.impl.ColumnNameTypeValueDeserializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Type;

/**
 * Created by ajnavarro on 12/01/15.
 */
public class ColumnNameTypeValueDeserializerTest {

    private final static String COLUMN_NAME_TYPE_VALUE_STRING_EXAMPLE_OK =
            "{\"column\":\"test\",\"type\":\"STRING\",\"value\":\"testString\"}";

    private final static String COLUMN_NAME_TYPE_VALUE_STRING_EXAMPLE_NO_VALUE_BUT_OK =
            "{\"column\":\"test\",\"type\":\"STRING\"}";

    private final static String COLUMN_NAME_TYPE_VALUE_STRING_EXAMPLE_KO = "{}";

    private ColumnNameTypeValueDeserializer columnNameTypeValueDeserializer;

    private JsonElement jsonElement;


    private Type type = Mockito.mock(Type.class);
    private JsonDeserializationContext ctx = Mockito.mock(JsonDeserializationContext.class);

    @Before
    public void before() {
        jsonElement = Mockito.mock(JsonElement.class);
        columnNameTypeValueDeserializer = new ColumnNameTypeValueDeserializer();
    }

    @After
    public void after() {
        jsonElement = null;
        columnNameTypeValueDeserializer = null;
    }

    @Test
    public void emptyJsonTest() {
        JsonParser parser = new JsonParser();
        JsonObject parsed = (JsonObject) parser.parse(COLUMN_NAME_TYPE_VALUE_STRING_EXAMPLE_KO);
        Mockito.when(jsonElement.getAsJsonObject()).thenReturn(parsed);

        ColumnNameTypeValue result = columnNameTypeValueDeserializer.deserialize(jsonElement, type, ctx);

        Assert.assertNull("Expected null but found a value", result.getColumn());
        Assert.assertNull("Expected null but found a value", result.getType());
        Assert.assertNull("Expected null but found a value", result.getValue());
    }

    @Test
    public void nullJsonObjectTest() {
        Mockito.when(jsonElement.getAsJsonObject()).thenReturn(null);

        ColumnNameTypeValue result = columnNameTypeValueDeserializer.deserialize(jsonElement, type, ctx);

        Assert.assertNull("Expected null but found a value", result.getColumn());
        Assert.assertNull("Expected null but found a value", result.getType());
        Assert.assertNull("Expected null but found a value", result.getValue());
    }

    @Test
    public void correctJsonTest() {
        JsonParser parser = new JsonParser();
        JsonObject parsed = (JsonObject) parser.parse(COLUMN_NAME_TYPE_VALUE_STRING_EXAMPLE_OK);
        Mockito.when(jsonElement.getAsJsonObject()).thenReturn(parsed);

        ColumnNameTypeValue result = columnNameTypeValueDeserializer.deserialize(jsonElement, type, ctx);

        Assert.assertNotNull("Expected value but found null", result.getColumn());
        Assert.assertNotNull("Expected value but found null", result.getType());
        Assert.assertNotNull("Expected value but found null", result.getValue());
    }

    @Test
    public void correctJsonButNoValueTest() {
        JsonParser parser = new JsonParser();
        JsonObject parsed = (JsonObject) parser.parse(COLUMN_NAME_TYPE_VALUE_STRING_EXAMPLE_NO_VALUE_BUT_OK);
        Mockito.when(jsonElement.getAsJsonObject()).thenReturn(parsed);

        ColumnNameTypeValue result = columnNameTypeValueDeserializer.deserialize(jsonElement, type, ctx);

        Assert.assertNotNull("Expected value but found null", result.getColumn());
        Assert.assertNotNull("Expected value but found null", result.getType());
        Assert.assertNull("Expected null but found a value", result.getValue());
    }
}
