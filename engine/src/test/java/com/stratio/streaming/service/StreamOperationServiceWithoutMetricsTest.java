package com.stratio.streaming.service;

import com.stratio.streaming.commons.constants.ColumnType;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.configuration.ServiceConfiguration;
import com.stratio.streaming.configuration.StreamingSiddhiConfiguration;
import com.stratio.streaming.dao.StreamStatusDao;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;

import java.util.*;

import static org.junit.Assert.*;

/**
 * Created by aitor on 9/16/15.
 */
public class StreamOperationServiceWithoutMetricsTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamOperationServiceWithoutMetricsTest.class);

    private SiddhiManager siddhiManager;

    private StreamStatusDao streamStatusDao;

    private CallbackService callbackService;

    private StreamOperationServiceWithoutMetrics streamOperationsService;

    private static final String STREAM_NAME= "testStream";
    private static final String RESULT_STREAM_NAME= "resultStream";


    private static final List<ColumnNameTypeValue> COLUMNS= new LinkedList<ColumnNameTypeValue>() {{
            add(new ColumnNameTypeValue("name", ColumnType.STRING, "name"));
            add(new ColumnNameTypeValue("timestamp", ColumnType.DOUBLE, "timestamp"));
            add(new ColumnNameTypeValue("value", ColumnType.INTEGER, "value"));
    }};

    @Before
    public void setUp() throws Exception {

        LOGGER.debug("Initializing required classes");
        siddhiManager= new StreamingSiddhiConfiguration().siddhiManager();
        streamStatusDao= new StreamStatusDao();
        //callbackService= new CallbackService(producer, kafkaToJavaSerializer(), javaToSiddhiSerializer());
        ServiceConfiguration serviceConfiguration= new ServiceConfiguration();
        callbackService= serviceConfiguration.callbackService();

        streamOperationsService= new StreamOperationService(siddhiManager, streamStatusDao, callbackService);

    }

    @After
    public void tearDown() throws Exception {
        siddhiManager.shutdown();
    }

    @Test
    public void testCreateInternalStream() throws Exception {
        streamOperationsService.createInternalStream(STREAM_NAME, COLUMNS);
        assertTrue(streamOperationsService.streamExist(STREAM_NAME));
    }

    @Test
    public void testCreateStream() throws Exception {
        streamOperationsService.createStream(STREAM_NAME, COLUMNS);
        assertTrue(streamOperationsService.streamExist(STREAM_NAME));
        assertTrue(streamOperationsService.isUserDefined(STREAM_NAME));
    }

    @Test
    public void testEnlargeStream() throws Exception {
        streamOperationsService.createInternalStream(STREAM_NAME, COLUMNS);
        streamOperationsService.enlargeStream(STREAM_NAME, new LinkedList<ColumnNameTypeValue>() {
            {
                add(new ColumnNameTypeValue("additionalColumn", ColumnType.STRING, "additionalColumn"));
            }
        });
        assertTrue(streamOperationsService.streamExist(STREAM_NAME));

    }

    @Test
    public void testDropStream() throws Exception {
        streamOperationsService.createStream(STREAM_NAME, COLUMNS);
        assertTrue(streamOperationsService.streamExist(STREAM_NAME));
        streamOperationsService.dropStream(STREAM_NAME);
        assertFalse(streamOperationsService.streamExist(STREAM_NAME));
    }

    @Test
    public void testAddQuery() throws Exception {
        streamOperationsService.createInternalStream(STREAM_NAME, COLUMNS);
        String query = " from "+ STREAM_NAME+ "[value >= 50] select name,timestamp,value insert into " + RESULT_STREAM_NAME+ " ;";
        streamOperationsService.addQuery(STREAM_NAME, query);

        assertTrue(streamOperationsService.streamExist(STREAM_NAME));
        assertTrue(streamOperationsService.queryRawExists(STREAM_NAME, query));
    }

}