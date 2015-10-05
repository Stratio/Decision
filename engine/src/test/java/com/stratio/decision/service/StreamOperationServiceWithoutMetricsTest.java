package com.stratio.decision.service;

import com.stratio.decision.commons.constants.ColumnType;
import com.stratio.decision.commons.constants.StreamAction;
import com.stratio.decision.commons.messages.ColumnNameTypeValue;
import com.stratio.decision.commons.messages.StratioStreamingMessage;
import com.stratio.decision.configuration.ServiceConfiguration;
import com.stratio.decision.configuration.StreamingSiddhiConfiguration;
import com.stratio.decision.dao.StreamStatusDao;
import com.stratio.decision.exception.ServiceException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;

import java.util.LinkedList;
import java.util.List;

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
        streamOperationsService.createInternalStream(StreamsHelper.STREAM_NAME, StreamsHelper.COLUMNS);
        assertTrue("Expected true not found", streamOperationsService.streamExist(StreamsHelper.STREAM_NAME));
    }

    @Test
    public void testCreateStream() throws Exception {
        streamOperationsService.createStream(StreamsHelper.STREAM_NAME, StreamsHelper.COLUMNS);
        assertTrue("Expected true not found", streamOperationsService.streamExist(StreamsHelper.STREAM_NAME));
        assertTrue("Expected true not found", streamOperationsService.isUserDefined(StreamsHelper.STREAM_NAME));
    }

    @Test
    public void testEnlargeStream() throws Exception {
        streamOperationsService.createInternalStream(StreamsHelper.STREAM_NAME, StreamsHelper.COLUMNS);
        streamOperationsService.enlargeStream(StreamsHelper.STREAM_NAME, new LinkedList<ColumnNameTypeValue>() {
            {
                add(new ColumnNameTypeValue("additionalColumn", ColumnType.STRING, "additionalColumn"));
            }
        });

        assertTrue("Expected true not found", streamOperationsService.streamExist(StreamsHelper.STREAM_NAME));

    }

    @Test(expected = ServiceException.class)
    public void testEnlargeStreamNotExistingColumn() throws Exception {
        streamOperationsService.createInternalStream(StreamsHelper.STREAM_NAME, StreamsHelper.COLUMNS);
        streamOperationsService.enlargeStream(StreamsHelper.STREAM_NAME, new LinkedList<ColumnNameTypeValue>() {
            {
                add(new ColumnNameTypeValue("name", ColumnType.STRING, "name"));
            }
        });

        assertTrue("Expected true not found", streamOperationsService.streamExist(StreamsHelper.STREAM_NAME));

    }



    @Test
    public void testDropStream() throws Exception {
        streamOperationsService.createStream(StreamsHelper.STREAM_NAME, StreamsHelper.COLUMNS);
        assertTrue(streamOperationsService.streamExist(StreamsHelper.STREAM_NAME));

        String queryId= streamOperationsService.addQuery(StreamsHelper.STREAM_NAME, StreamsHelper.QUERY);

        streamOperationsService.dropStream(StreamsHelper.STREAM_NAME);
        assertFalse("Expected false not found", streamOperationsService.streamExist(StreamsHelper.STREAM_NAME));
    }

    @Test
    public void testAddAndRemoveQuery() throws Exception {
        streamOperationsService.createInternalStream(StreamsHelper.STREAM_NAME, StreamsHelper.COLUMNS);
        String queryId= streamOperationsService.addQuery(StreamsHelper.STREAM_NAME, StreamsHelper.QUERY);

        assertTrue("Expected true not found", streamOperationsService.streamExist(StreamsHelper.STREAM_NAME));
        assertTrue("Expected true not found", streamOperationsService.queryRawExists(StreamsHelper.STREAM_NAME, StreamsHelper.QUERY));
        assertFalse("Expected false not found", streamOperationsService.queryRawExists(StreamsHelper.STREAM_NAME, "notExistingQuery"));
        assertTrue("Expected true not found", streamOperationsService.queryIdExists(StreamsHelper.STREAM_NAME, queryId));
        assertFalse("Expected false not found", streamOperationsService.queryIdExists(StreamsHelper.STREAM_NAME, "notExistingQueryAgain"));

        streamOperationsService.removeQuery(queryId, StreamsHelper.STREAM_NAME);
    }

    @Test
    public void testActions() throws Exception {
        streamOperationsService.createStream(StreamsHelper.STREAM_NAME, StreamsHelper.COLUMNS);
        String queryId= streamOperationsService.addQuery(StreamsHelper.STREAM_NAME, StreamsHelper.QUERY);

        streamOperationsService.enableAction(StreamsHelper.STREAM_NAME, StreamAction.LISTEN);
        assertTrue("Expected true not found", streamOperationsService.isActionEnabled(StreamsHelper.STREAM_NAME, StreamAction.LISTEN));
        streamOperationsService.disableAction(StreamsHelper.STREAM_NAME, StreamAction.LISTEN);
        assertFalse("Expected false not found", streamOperationsService.isActionEnabled(StreamsHelper.STREAM_NAME, StreamAction.LISTEN));


    }

    @Test
    public void testList() throws Exception {
        streamOperationsService.createStream(StreamsHelper.STREAM_NAME, StreamsHelper.COLUMNS);
        String queryId= streamOperationsService.addQuery(StreamsHelper.STREAM_NAME, StreamsHelper.QUERY);

        streamOperationsService.enableAction(StreamsHelper.STREAM_NAME, StreamAction.LISTEN);

        List<StratioStreamingMessage> list= streamOperationsService.list();

        for (StratioStreamingMessage message: list) {
            if (message.getStreamName().equals(StreamsHelper.STREAM_NAME)) {
                assertEquals("Expected value not found", 1, message.getQueries().size());
                assertEquals("Expected value not found", 6, message.getColumns().size());
            } else {
                assertEquals("Expected value not found", 0, message.getActiveActions().size());
                assertEquals("Expected value not found", 3, message.getColumns().size());
            }
        }


    }
}