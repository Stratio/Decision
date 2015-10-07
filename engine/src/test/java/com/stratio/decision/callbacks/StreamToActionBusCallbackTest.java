package com.stratio.decision.callbacks;

import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.doNothing;

import java.util.List;
import java.util.Set;

import org.apache.commons.collections.set.ListOrderedSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;

import com.stratio.decision.commons.constants.StreamAction;
import com.stratio.decision.commons.messages.StratioStreamingMessage;
import com.stratio.decision.configuration.StreamingSiddhiConfiguration;
import com.stratio.decision.factory.GsonFactory;
import com.stratio.decision.serializer.impl.JavaToSiddhiSerializer;
import com.stratio.decision.serializer.impl.KafkaToJavaSerializer;
import com.stratio.decision.service.StreamMetadataService;
import com.stratio.decision.service.StreamsHelper;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

/**
 * Created by aitor on 10/7/15.
 */
public class StreamToActionBusCallbackTest {

    private JavaToSiddhiSerializer javaToSiddhiSerializer;
    private KafkaToJavaSerializer kafkaToJavaSerializer;
    private StreamMetadataService metadataService;
    private SiddhiManager siddhiManager;
    private StreamToActionBusCallback cbk;
    private static final String streamName= "testStream";
    @Mock
    private Producer<String, String> producer;

    @Before
    public void setUp() throws Exception {
        siddhiManager= new StreamingSiddhiConfiguration().siddhiManager();
        siddhiManager.defineStream(StreamsHelper.STREAM_DEFINITION);
        metadataService= new StreamMetadataService(siddhiManager);
        javaToSiddhiSerializer= new JavaToSiddhiSerializer(metadataService);
        kafkaToJavaSerializer= new KafkaToJavaSerializer(GsonFactory.getInstance());

        Set<StreamAction> activeActions= new ListOrderedSet();
        activeActions.add(StreamAction.LISTEN);

        producer= Mockito.mock(Producer.class);
        //List<KeyedMessage<String, String>> km= any();
        //doNothing().when(producer).send(km);
        doNothing().when(producer).send(Matchers.<List<KeyedMessage<String, String>>>any());

        cbk= new StreamToActionBusCallback(activeActions, streamName, producer,
                kafkaToJavaSerializer, javaToSiddhiSerializer);
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testReceive() throws Exception {
        StratioStreamingMessage message= StreamsHelper.getSampleMessage();
        Event event= javaToSiddhiSerializer.serialize(message);
        Event[] inEvents= {event};
        Event[] removeEvents= {event};

        Exception ex= null;
        try {
            cbk.receive(123456789L, inEvents, removeEvents);
        } catch (Exception e) {ex= e;}

        assertNull("Unexpected exception found", ex);

    }
}