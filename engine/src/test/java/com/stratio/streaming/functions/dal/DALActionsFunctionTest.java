package com.stratio.streaming.functions.dal;

import com.stratio.streaming.commons.constants.STREAM_OPERATIONS;
import com.stratio.streaming.functions.ActionBaseFunctionHelper;
import com.stratio.streaming.functions.ddl.AddQueryToStreamFunction;
import com.stratio.streaming.service.StreamsHelper;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.junit.Assert.*;

/**
 * Created by aitor on 9/23/15.
 */
public class DALActionsFunctionTest extends ActionBaseFunctionHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(DALActionsFunctionTest.class);



    @Before
    public void setUp() throws Exception {
        LOGGER.debug("Initializing required classes");
        initialize();
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