package com.stratio.streaming.functions.dal;

import com.stratio.streaming.commons.constants.STREAM_OPERATIONS;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.configuration.ServiceConfiguration;
import com.stratio.streaming.configuration.StreamingSiddhiConfiguration;
import com.stratio.streaming.dao.StreamStatusDao;
import com.stratio.streaming.functions.validator.RequestValidation;
import com.stratio.streaming.functions.validator.StreamNameNotEmptyValidation;
import com.stratio.streaming.service.CallbackService;
import com.stratio.streaming.service.StreamOperationService;
import com.stratio.streaming.service.StreamsHelper;
import org.apache.commons.collections.set.ListOrderedSet;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Created by aitor on 9/23/15.
 */
public class ActionsFunctionTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ActionsFunctionTest.class);

    private SiddhiManager siddhiManager;

    private StreamStatusDao streamStatusDao;

    private CallbackService callbackService;

    private StreamOperationService streamOperationsService;

    private Set<RequestValidation> validators;

    private static final String ZOO_HOST= "localhost";

    @Before
    public void setUp() throws Exception {
        LOGGER.debug("Initializing required classes");
        siddhiManager= new StreamingSiddhiConfiguration().siddhiManager();
        streamStatusDao= new StreamStatusDao();
        ServiceConfiguration serviceConfiguration= new ServiceConfiguration();
        callbackService= serviceConfiguration.callbackService();

        streamOperationsService= new StreamOperationService(siddhiManager, streamStatusDao, callbackService);

        streamOperationsService.createStream(StreamsHelper.STREAM_NAME, StreamsHelper.COLUMNS);
        String queryId= streamOperationsService.addQuery(StreamsHelper.STREAM_NAME, StreamsHelper.QUERY);
        StratioStreamingMessage message= StreamsHelper.getSampleMessage();

        validators= new ListOrderedSet();
        StreamNameNotEmptyValidation validation= new StreamNameNotEmptyValidation();
        validators.add(validation);
    }

    @Test
    public void testListeStreamFunction() throws Exception {

        ListenStreamFunction func= new ListenStreamFunction(streamOperationsService, ZOO_HOST);
        assertEquals(STREAM_OPERATIONS.ACTION.LISTEN, func.getStartOperationCommand());
        assertEquals(STREAM_OPERATIONS.ACTION.STOP_LISTEN, func.getStopOperationCommand());

        assertTrue(func.startAction(StreamsHelper.getSampleMessage()));
        assertTrue(func.stopAction(StreamsHelper.getSampleMessage()));

        func.addStartRequestsValidations(validators);
        func.addStopRequestsValidations(validators);
    }


    @Test
    public void testIndexStreamFunction() throws Exception {

        IndexStreamFunction func= new IndexStreamFunction(streamOperationsService, ZOO_HOST);
        assertEquals(STREAM_OPERATIONS.ACTION.INDEX, func.getStartOperationCommand());
        assertEquals(STREAM_OPERATIONS.ACTION.STOP_INDEX, func.getStopOperationCommand());

        assertTrue(func.startAction(StreamsHelper.getSampleMessage()));
        assertTrue(func.stopAction(StreamsHelper.getSampleMessage()));

        func.addStartRequestsValidations(validators);
        func.addStopRequestsValidations(validators);
    }


    @Test
    public void testSaveToCassandraStreamFunction() throws Exception {

        SaveToCassandraStreamFunction func= new SaveToCassandraStreamFunction(streamOperationsService, ZOO_HOST);
        assertEquals(STREAM_OPERATIONS.ACTION.SAVETO_CASSANDRA, func.getStartOperationCommand());
        assertEquals(STREAM_OPERATIONS.ACTION.STOP_SAVETO_CASSANDRA, func.getStopOperationCommand());

        assertTrue(func.startAction(StreamsHelper.getSampleMessage()));
        assertTrue(func.stopAction(StreamsHelper.getSampleMessage()));

        func.addStartRequestsValidations(validators);
        func.addStopRequestsValidations(validators);
    }

    @Test
    public void testSaveToMongoStreamFunction() throws Exception {

        SaveToMongoStreamFunction func= new SaveToMongoStreamFunction(streamOperationsService, ZOO_HOST);
        assertEquals(STREAM_OPERATIONS.ACTION.SAVETO_MONGO, func.getStartOperationCommand());
        assertEquals(STREAM_OPERATIONS.ACTION.STOP_SAVETO_MONGO, func.getStopOperationCommand());

        assertTrue(func.startAction(StreamsHelper.getSampleMessage()));
        assertTrue(func.stopAction(StreamsHelper.getSampleMessage()));

        func.addStartRequestsValidations(validators);
        func.addStopRequestsValidations(validators);
    }

    @Test
    public void testSaveToSolrStreamFunction() throws Exception {

        SaveToSolrStreamFunction func= new SaveToSolrStreamFunction(streamOperationsService, ZOO_HOST);
        assertEquals(STREAM_OPERATIONS.ACTION.SAVETO_SOLR, func.getStartOperationCommand());
        assertEquals(STREAM_OPERATIONS.ACTION.STOP_SAVETO_SOLR, func.getStopOperationCommand());

        assertTrue(func.startAction(StreamsHelper.getSampleMessage()));
        assertTrue(func.stopAction(StreamsHelper.getSampleMessage()));

        func.addStartRequestsValidations(validators);
        func.addStopRequestsValidations(validators);
    }

}