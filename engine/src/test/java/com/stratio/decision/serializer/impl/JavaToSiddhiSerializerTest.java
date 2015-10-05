package com.stratio.decision.serializer.impl;

import com.stratio.decision.commons.constants.ColumnType;
import com.stratio.decision.commons.messages.ColumnNameTypeValue;
import com.stratio.decision.commons.messages.StratioStreamingMessage;
import com.stratio.decision.configuration.StreamingSiddhiConfiguration;
import com.stratio.decision.service.StreamMetadataService;
import com.stratio.decision.service.StreamsHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.event.in.InEvent;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by aitor on 9/24/15.
 */
public class JavaToSiddhiSerializerTest {

    private JavaToSiddhiSerializer serializer;
    private StreamMetadataService metadataService;
    private SiddhiManager siddhiManager;

    @Before
    public void setUp() throws Exception {
        siddhiManager= new StreamingSiddhiConfiguration().siddhiManager();
        siddhiManager.defineStream(StreamsHelper.STREAM_DEFINITION);
        metadataService= new StreamMetadataService(siddhiManager);
        serializer= new JavaToSiddhiSerializer(metadataService);
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testSerialize() throws Exception {
        StratioStreamingMessage message= StreamsHelper.getSampleMessage();
        Event event= serializer.serialize(message);
        assertEquals("Expected value not found", "name", event.getData(0));
        assertEquals("Expected value not found", 3, event.getData(2));
        assertEquals("Expected value not found", 13f, event.getData(5));
    }

    @Test
    public void testSerializeList() throws Exception {
        List<StratioStreamingMessage> messages= new ArrayList<>();

        messages.add(StreamsHelper.getSampleMessage());
        messages.add(StreamsHelper.getSampleMessage());

        List<Event> events= serializer.serialize(messages);

        assertEquals("Expected value not found", 2, events.size());

    }

    @Test
    public void testDeserialize() throws Exception {
        Object[] values = new Object[5];
        values[0]= new ColumnNameTypeValue("name", ColumnType.STRING, "test");
        values[1]= new ColumnNameTypeValue("value", ColumnType.INTEGER, 10);
        values[2]= new ColumnNameTypeValue("enabled", ColumnType.BOOLEAN, true);
        values[3]= new ColumnNameTypeValue("numberl", ColumnType.LONG, 123L);
        values[4]= new ColumnNameTypeValue("numberf", ColumnType.FLOAT, 13f);

        Event event= new InEvent(StreamsHelper.STREAM_NAME, System.currentTimeMillis(), values);
        StratioStreamingMessage message= serializer.deserialize(event);

        assertEquals("Expected value not found", "test",
                ((ColumnNameTypeValue) message.getColumns().get(0).getValue()).getValue());
        assertEquals("Expected value not found",10,
                ((ColumnNameTypeValue) message.getColumns().get(1).getValue()).getValue());
        assertEquals("Expected value not found", true,
                ((ColumnNameTypeValue) message.getColumns().get(2).getValue()).getValue());
    }


    @Test
    public void testDeserializeList() throws Exception {
        List<Event> events= new ArrayList<Event>();

        Object[] values = new Object[3];
        values[0]= new ColumnNameTypeValue("name", ColumnType.STRING, "test");
        values[1]= new ColumnNameTypeValue("value", ColumnType.INTEGER, 10);
        values[2]= new ColumnNameTypeValue("enabled", ColumnType.BOOLEAN, true);
        events.add(new InEvent(StreamsHelper.STREAM_NAME, System.currentTimeMillis(), values));

        values = new Object[3];
        values[0]= new ColumnNameTypeValue("name", ColumnType.STRING, "test2");
        values[1]= new ColumnNameTypeValue("value", ColumnType.INTEGER, 11);
        values[2]= new ColumnNameTypeValue("enabled", ColumnType.BOOLEAN, false);
        events.add(new InEvent(StreamsHelper.STREAM_NAME, System.currentTimeMillis(), values));

        List<StratioStreamingMessage> messages= serializer.deserialize(events);
        assertEquals("Expected value not found", 2, messages.size());
    }
}