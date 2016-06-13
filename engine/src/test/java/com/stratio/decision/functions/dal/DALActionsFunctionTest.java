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
package com.stratio.decision.functions.dal;

import com.stratio.decision.commons.constants.STREAM_OPERATIONS;
import com.stratio.decision.commons.messages.StratioStreamingMessage;
import com.stratio.decision.functions.ActionBaseFunctionHelper;
import com.stratio.decision.service.StreamsHelper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
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
    private static JavaSparkContext context= null;


    @Before
    public void setUp() throws Exception {
        LOGGER.debug("Initializing required classes");
        initialize();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        try {
            if (context instanceof JavaSparkContext)
                context.stop();
        } catch (Exception ex) {}
    }

    @Test
    public void testListeStreamFunction() throws Exception {

        ListenStreamFunction func= new ListenStreamFunction(streamOperationsService, ZOO_HOST);

        assertEquals("Expected action not found", STREAM_OPERATIONS.ACTION.LISTEN, func.getStartOperationCommand());
        assertEquals("Expected action not found", STREAM_OPERATIONS.ACTION.STOP_LISTEN, func.getStopOperationCommand());

        assertTrue("Expected true value", func.startAction(StreamsHelper.getSampleMessage()));
        assertTrue("Expected true value", func.stopAction(StreamsHelper.getSampleMessage()));

        func.addStartRequestsValidations(validators);
        func.addStopRequestsValidations(validators);
    }


    @Test
    public void testIndexStreamFunction() throws Exception {

        IndexStreamFunction func= new IndexStreamFunction(streamOperationsService, ZOO_HOST);
        assertEquals("Expected action not found", STREAM_OPERATIONS.ACTION.INDEX, func.getStartOperationCommand());
        assertEquals("Expected action not found", STREAM_OPERATIONS.ACTION.STOP_INDEX, func.getStopOperationCommand());

        assertTrue("Expected true value", func.startAction(StreamsHelper.getSampleMessage()));
        assertTrue("Expected true value", func.stopAction(StreamsHelper.getSampleMessage()));

        func.addStartRequestsValidations(validators);
        func.addStopRequestsValidations(validators);
    }


    @Test
    public void testSaveToCassandraStreamFunction() throws Exception {

        SaveToCassandraStreamFunction func= new SaveToCassandraStreamFunction(streamOperationsService, ZOO_HOST);
        assertEquals("Expected action not found", STREAM_OPERATIONS.ACTION.SAVETO_CASSANDRA, func.getStartOperationCommand());
        assertEquals("Expected action not found", STREAM_OPERATIONS.ACTION.STOP_SAVETO_CASSANDRA, func.getStopOperationCommand());

        assertTrue("Expected true value", func.startAction(StreamsHelper.getSampleMessage()));
        assertTrue("Expected true value", func.stopAction(StreamsHelper.getSampleMessage()));

        func.addStartRequestsValidations(validators);
        func.addStopRequestsValidations(validators);
    }


    @Test
    public void testSaveToMongoStreamFunction() throws Exception {

        SaveToMongoStreamFunction func= new SaveToMongoStreamFunction(streamOperationsService, ZOO_HOST);
        assertEquals("Expected action not found", STREAM_OPERATIONS.ACTION.SAVETO_MONGO, func.getStartOperationCommand());
        assertEquals("Expected action not found", STREAM_OPERATIONS.ACTION.STOP_SAVETO_MONGO, func.getStopOperationCommand());

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

        message.setOperation("stop_listen");
        list.add(message);

        context = new JavaSparkContext("local[2]", "test");
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
