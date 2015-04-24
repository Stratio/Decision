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
package com.stratio.streaming.unit.engine.shell.commands;

import com.stratio.streaming.commons.exceptions.*;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.shell.core.CommandResult;

import static org.junit.Assert.assertEquals;

public class ErrorHandlingTest extends BaseShellTest {

    private final static String GOOD_ERROR_MESSAGE = "GOOD ERROR MESSAGE";
    private final static String REPEATED_QUERY = "REPEATED_QUERY";

    private final static String STREAM_NAME = "STREAM_NAME";

    @Before
    public void setUp() {
        init();
    }

    @Test
    public void listWith0StreamsTest() throws StratioEngineStatusException, StratioAPIGenericException,
            StratioAPISecurityException, StratioEngineOperationException, StratioEngineConnectionException {
        Mockito.when(ssaw.api().addQuery(Mockito.anyString(), Mockito.anyString())).thenThrow(
                new StratioEngineOperationException(GOOD_ERROR_MESSAGE));

        CommandResult cr = shell
                .executeCommand("add query --stream " + STREAM_NAME + " --definition " + REPEATED_QUERY);
        assertEquals(false, cr.isSuccess());
        assertEquals("-> " + GOOD_ERROR_MESSAGE, cr.getException().toString());
    }
}
