package com.stratio.streaming.shell.commands;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.shell.core.CommandResult;

import com.stratio.streaming.commons.exceptions.StratioAPIGenericException;
import com.stratio.streaming.commons.exceptions.StratioAPISecurityException;
import com.stratio.streaming.commons.exceptions.StratioEngineOperationException;
import com.stratio.streaming.commons.exceptions.StratioEngineStatusException;

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
            StratioAPISecurityException, StratioEngineOperationException {
        Mockito.when(stratioStreamingApi.addQuery(Mockito.anyString(), Mockito.anyString())).thenThrow(
                new StratioEngineOperationException(GOOD_ERROR_MESSAGE));

        CommandResult cr = shell
                .executeCommand("add query --stream " + STREAM_NAME + " --definition " + REPEATED_QUERY);
        assertEquals(false, cr.isSuccess());
        assertEquals("-> " + GOOD_ERROR_MESSAGE, cr.getException().toString());
    }
}
