package com.stratio.streaming.functions.dml;

import com.stratio.streaming.commons.constants.ColumnType;
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.functions.ActionBaseFunctionHelper;
import com.stratio.streaming.functions.ddl.AddQueryToStreamFunction;
import com.stratio.streaming.functions.ddl.AlterStreamFunction;
import com.stratio.streaming.functions.ddl.CreateStreamFunction;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by aitor on 9/23/15.
 */
public class DMLActionsFunctionTest extends ActionBaseFunctionHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(DMLActionsFunctionTest.class);



    @Before
    public void setUp() throws Exception {
        LOGGER.debug("Initializing required classes");
        initialize();
    }

    @Test
    public void testIntoStreamFunction() throws Exception {

        InsertIntoStreamFunction func= new InsertIntoStreamFunction(streamOperationsService, ZOO_HOST);
        assertEquals(STREAM_OPERATIONS.MANIPULATION.INSERT, func.getStartOperationCommand());
        assertEquals(null, func.getStopOperationCommand());

        Exception ex= null;
        try {
            func.startAction(message);
            assertTrue(func.stopAction(message));
        } catch (Exception e)   { ex= e;}
        assertEquals(null, ex);


        func.addStartRequestsValidations(validators);
        func.addStopRequestsValidations(validators);
    }


    @Test
    public void testListStreamFunction() throws Exception {

        ListStreamsFunction func= new ListStreamsFunction(streamOperationsService, ZOO_HOST);
        assertEquals(STREAM_OPERATIONS.MANIPULATION.LIST, func.getStartOperationCommand());
        assertEquals(null, func.getStopOperationCommand());

        func.addStartRequestsValidations(validators);
        func.addStopRequestsValidations(validators);
    }

}