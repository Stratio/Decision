package com.stratio.streaming.functions.messages;

import com.stratio.streaming.commons.constants.STREAM_OPERATIONS;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.service.StreamsHelper;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import static org.junit.Assert.*;

/**
 * Created by aitor on 9/25/15.
 */
public class MessagesFunctionTest {

    @Before
    public void setUp() throws Exception {

    }

    @Test
    public void testFilterMessagesByOperation() throws Exception {
        String operation= "MyOperation";
        FilterMessagesByOperationFunction func= new FilterMessagesByOperationFunction(operation);
        Tuple2<String, String> message= new Tuple2<>(operation.toLowerCase(), "text");
        assertTrue("Expected true value but found false", func.call(message));

        message= new Tuple2<>("bla bla", "text");
        assertFalse("Expected false value but found true", func.call(message));
    }


    @Test
    public void testKeppPayloadFromMessage() throws Exception {
        String json= "{\"streamName\":\"testStream\",\"timestamp\":1234567890,\"columns\":[{\"column\":\"name\",\"type\":\"STRING\",\"value\":\"name\"},{\"column\":\"timestamp\",\"type\":\"DOUBLE\",\"value\":\"0.0\"},{\"column\":\"value\",\"type\":\"INTEGER\",\"value\":3},{\"column\":\"enabled\",\"type\":\"BOOLEAN\",\"value\":true},{\"column\":\"numberl\",\"type\":\"LONG\",\"value\":123},{\"column\":\"numberf\",\"type\":\"FLOAT\",\"value\":13.0}],\"queries\":[],\"activeActions\":[\"LISTEN\"]}";

        KeepPayloadFromMessageFunction func= new KeepPayloadFromMessageFunction();
        Tuple2<String, String> tuple= new Tuple2<>(STREAM_OPERATIONS.ACTION.LISTEN, json);

        StratioStreamingMessage message= func.call(tuple);
        assertEquals("Expected streamName not found", "testStream", message.getStreamName());
        assertEquals("Expected value not found", "name", message.getColumns().get(0).getValue());
    }
}