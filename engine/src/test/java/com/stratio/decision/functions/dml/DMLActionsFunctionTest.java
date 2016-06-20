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
package com.stratio.decision.functions.dml;

import com.stratio.decision.commons.constants.STREAM_OPERATIONS;
import com.stratio.decision.functions.ActionBaseFunctionHelper;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        assertEquals("Expected operation not found", STREAM_OPERATIONS.MANIPULATION.INSERT, func.getStartOperationCommand());
        assertEquals("Expected null value not found", null, func.getStopOperationCommand());

        Exception ex= null;
        try {
            func.startAction(message);
            assertTrue("Expected true not found", func.stopAction(message));
        } catch (Exception e)   { ex= e;}
        assertEquals("Expected not exception raised", null, ex);


        func.addStartRequestsValidations(validators);
        func.addStopRequestsValidations(validators);
    }


    @Test
    public void testListStreamFunction() throws Exception {

        ListStreamsFunction func= new ListStreamsFunction(streamOperationsService, ZOO_HOST);
        assertEquals("Expected operation not found",
                STREAM_OPERATIONS.MANIPULATION.LIST, func.getStartOperationCommand());
        assertEquals("Expected null value not found", null, func.getStopOperationCommand());

        func.addStartRequestsValidations(validators);
        func.addStopRequestsValidations(validators);
    }

}