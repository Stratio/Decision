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
import com.stratio.decision.factory.GsonFactory;
import com.stratio.decision.service.StreamsHelper;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

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