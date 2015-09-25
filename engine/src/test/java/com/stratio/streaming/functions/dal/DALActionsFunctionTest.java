package com.stratio.streaming.functions.dal;

import com.stratio.streaming.commons.constants.STREAM_OPERATIONS;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.functions.ActionBaseFunctionHelper;
import com.stratio.streaming.service.StreamsHelper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;

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

        assertTrue("Expected true value", func.startAction(StreamsHelper.getSampleMessage()));
        assertTrue("Expected true value", func.stopAction(StreamsHelper.getSampleMessage()));

        func.addStartRequestsValidations(validators);
        func.addStopRequestsValidations(validators);
    }


    @Test
    public void testIndexStreamFunction() throws Exception {

        IndexStreamFunction func= new IndexStreamFunction(streamOperationsService, ZOO_HOST);
        assertEquals(STREAM_OPERATIONS.ACTION.INDEX, func.getStartOperationCommand());
        assertEquals(STREAM_OPERATIONS.ACTION.STOP_INDEX, func.getStopOperationCommand());

        assertTrue("Expected true value", func.startAction(StreamsHelper.getSampleMessage()));
        assertTrue("Expected true value", func.stopAction(StreamsHelper.getSampleMessage()));

        func.addStartRequestsValidations(validators);
        func.addStopRequestsValidations(validators);
    }


    @Test
    public void testSaveToCassandraStreamFunction() throws Exception {

        SaveToCassandraStreamFunction func= new SaveToCassandraStreamFunction(streamOperationsService, ZOO_HOST);
        assertEquals(STREAM_OPERATIONS.ACTION.SAVETO_CASSANDRA, func.getStartOperationCommand());
        assertEquals(STREAM_OPERATIONS.ACTION.STOP_SAVETO_CASSANDRA, func.getStopOperationCommand());

        assertTrue("Expected true value", func.startAction(StreamsHelper.getSampleMessage()));
        assertTrue("Expected true value", func.stopAction(StreamsHelper.getSampleMessage()));

        func.addStartRequestsValidations(validators);
        func.addStopRequestsValidations(validators);
    }

    @Test
    public void testSaveToMongoStreamFunction() throws Exception {

        SaveToMongoStreamFunction func= new SaveToMongoStreamFunction(streamOperationsService, ZOO_HOST);
        assertEquals(STREAM_OPERATIONS.ACTION.SAVETO_MONGO, func.getStartOperationCommand());
        assertEquals(STREAM_OPERATIONS.ACTION.STOP_SAVETO_MONGO, func.getStopOperationCommand());

        assertTrue("Expected true value", func.startAction(StreamsHelper.getSampleMessage()));
        assertTrue("Expected true value", func.stopAction(StreamsHelper.getSampleMessage()));

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

    @Test
    @Ignore
    public void testActionBaseFunctionCall() throws Exception {

        List<StratioStreamingMessage> list= new ArrayList<StratioStreamingMessage>();
        list.add(message);

        JavaSparkContext context = new JavaSparkContext("local[2]", "test");
        //JavaRDD<StratioStreamingMessage> rdd= context.emptyRDD();
        JavaRDD<StratioStreamingMessage> rdd= context.parallelize(list);

        ListenStreamFunction func= new ListenStreamFunction(streamOperationsService, ZOO_HOST);
        Exception ex= null;
        try {
            func.startAction(message);
            func.call(rdd);
        } catch (Exception e) { ex= e; }

        assertNull("Expected null value", ex);
        context.stop();

    }
}