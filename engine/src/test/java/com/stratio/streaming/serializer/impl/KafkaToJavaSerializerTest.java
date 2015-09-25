package com.stratio.streaming.serializer.impl;

import com.google.gson.Gson;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.factory.GsonFactory;
import com.stratio.streaming.service.StreamsHelper;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by aitor on 9/24/15.
 */
public class KafkaToJavaSerializerTest {

    private KafkaToJavaSerializer serializer;

    @Before
    public void setUp() throws Exception {
        serializer= new KafkaToJavaSerializer(GsonFactory.getInstance());
    }

    @Test
    public void testDeserialize() throws Exception {
        StratioStreamingMessage message= StreamsHelper.getSampleMessage();
        String result= serializer.deserialize(message);
        Gson gson = new Gson();
        StratioStreamingMessage des= gson.fromJson(result, StratioStreamingMessage.class);

        assertEquals("Expected stream name not found", message.getStreamName(), des.getStreamName());
        assertEquals("Expected size not found", message.getColumns().size(), des.getColumns().size());

    }

    @Test
    public void testDeserializeList() throws Exception {
        List<StratioStreamingMessage> messages= new ArrayList<StratioStreamingMessage>();
        messages.add(StreamsHelper.getSampleMessage());
        messages.add(StreamsHelper.getSampleMessage());
        List<String> results= serializer.deserialize(messages);

        assertEquals("Expected size not found", 2, results.size());
    }

    @Test
    public void testSeserialize() throws Exception {
        String doc= "{\"streamName\":\"testStream\",\"timestamp\":1234567890,\"columns\":[{\"column\":\"name\",\"type\":\"STRING\",\"value\":\"name\"},{\"column\":\"timestamp\",\"type\":\"DOUBLE\",\"value\":\"0.0\"},{\"column\":\"value\",\"type\":\"INTEGER\",\"value\":3},{\"column\":\"enabled\",\"type\":\"BOOLEAN\",\"value\":true},{\"column\":\"numberl\",\"type\":\"LONG\",\"value\":123},{\"column\":\"numberf\",\"type\":\"FLOAT\",\"value\":13.0}],\"queries\":[],\"activeActions\":[\"LISTEN\"]}";

        StratioStreamingMessage message= serializer.serialize(doc);
        assertEquals("Expected stream name not found", "testStream", message.getStreamName());
        assertEquals("Expected value not found", "name", message.getColumns().get(0).getValue());
    }

    @Test
    public void testSerializeList() throws Exception {
        List<String> docs= new ArrayList<>();
        String doc= "{\"streamName\":\"testStream\",\"timestamp\":1234567890,\"columns\":[{\"column\":\"name\",\"type\":\"STRING\",\"value\":\"name\"},{\"column\":\"timestamp\",\"type\":\"DOUBLE\",\"value\":\"0.0\"},{\"column\":\"value\",\"type\":\"INTEGER\",\"value\":3},{\"column\":\"enabled\",\"type\":\"BOOLEAN\",\"value\":true},{\"column\":\"numberl\",\"type\":\"LONG\",\"value\":123},{\"column\":\"numberf\",\"type\":\"FLOAT\",\"value\":13.0}],\"queries\":[],\"activeActions\":[\"LISTEN\"]}";
        String doc2= "{\"streamName\":\"testStream\",\"timestamp\":1234567890,\"columns\":[{\"column\":\"name\",\"type\":\"STRING\",\"value\":\"name2\"},{\"column\":\"timestamp\",\"type\":\"DOUBLE\",\"value\":\"0.0\"},{\"column\":\"value\",\"type\":\"INTEGER\",\"value\":3},{\"column\":\"enabled\",\"type\":\"BOOLEAN\",\"value\":true},{\"column\":\"numberl\",\"type\":\"LONG\",\"value\":123},{\"column\":\"numberf\",\"type\":\"FLOAT\",\"value\":13.0}],\"queries\":[],\"activeActions\":[\"LISTEN\"]}";


        docs.add(doc);
        docs.add(doc2);

        List<StratioStreamingMessage> messages= serializer.serialize(docs);
        assertEquals(2, messages.size());
        assertEquals("Expected stream name not found", "testStream", messages.get(0).getStreamName());
        assertEquals("Expected stream name not found", "testStream", messages.get(1).getStreamName());
    }
}