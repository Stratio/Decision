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
package com.stratio.streaming.shell.commands;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.shell.Bootstrap;
import org.springframework.shell.core.CommandResult;
import org.springframework.shell.core.JLineShellComponent;

import com.stratio.streaming.api.IStratioStreamingAPI;
import com.stratio.streaming.commons.constants.ColumnType;
import com.stratio.streaming.commons.exceptions.StratioAPISecurityException;
import com.stratio.streaming.commons.exceptions.StratioEngineOperationException;
import com.stratio.streaming.commons.exceptions.StratioEngineStatusException;
import com.stratio.streaming.commons.streams.StratioStream;
import com.stratio.streaming.messaging.ColumnNameType;

public class StreamCommandTest {

    private static JLineShellComponent shell;

    private static IStratioStreamingAPI stratioStreamingApi;

    @BeforeClass
    public static void setUp() {
        Bootstrap bootstrap = new Bootstrap(null,
                new String[] { "classpath*:/META-INF/spring/spring-shell-plugin-test.xml" });
        shell = bootstrap.getJLineShellComponent();

        stratioStreamingApi = bootstrap.getApplicationContext().getBean(IStratioStreamingAPI.class);
    }

    @Test
    public void listWith0StreamsTest() throws StratioEngineStatusException {
        Mockito.when(stratioStreamingApi.listStreams()).thenReturn(new ArrayList<StratioStream>());
        CommandResult cr = shell.executeCommand("list");
        assertEquals(true, cr.isSuccess());
    }

    @Test
    public void createStreamTest() throws StratioEngineStatusException, StratioAPISecurityException,
            StratioEngineOperationException {
        // TODO
        String streamName = "testStream";
        List<ColumnNameType> values = new ArrayList<>();
        values.add(new ColumnNameType("column1", ColumnType.STRING));
        values.add(new ColumnNameType("column2", ColumnType.INTEGER));
        values.add(new ColumnNameType("column3", ColumnType.BOOLEAN));

        Mockito.doNothing().when(stratioStreamingApi).createStream(streamName, values);
        CommandResult cr = shell.executeCommand("list");
        assertEquals(true, cr.isSuccess());
    }
}
