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
package com.stratio.streaming.test.engine.shell.commands;

import com.stratio.streaming.commons.constants.ColumnType;
import com.stratio.streaming.commons.constants.StreamAction;
import com.stratio.streaming.commons.exceptions.*;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.commons.messages.StreamQuery;
import com.stratio.streaming.commons.streams.StratioStream;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.shell.core.CommandResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class StreamCommandTest extends BaseShellTest {

    @Before
    public void setUp() {
        init();
    }

    @Test
    public void listWith0StreamsTest() throws StratioEngineStatusException, StratioAPIGenericException, IOException, StratioEngineConnectionException {
        Mockito.when(ssaw.api().listStreams()).thenReturn(new ArrayList<StratioStream>());
        CommandResult cr = shell.executeCommand("list");
        assertEquals(true, cr.isSuccess());
        assertEquals(getListResultFromName("listWithNoStreams"), cr.getResult());
    }

    @Test
    public void listWithOneStreamTest() throws StratioEngineStatusException, StratioAPIGenericException,
            StratioAPISecurityException, StratioEngineOperationException, IOException, StratioEngineConnectionException {
        String streamName = "testStream";
        List<ColumnNameTypeValue> values = new ArrayList<>();
        values.add(new ColumnNameTypeValue("column1", ColumnType.STRING, null));
        values.add(new ColumnNameTypeValue("column2", ColumnType.INTEGER, null));
        values.add(new ColumnNameTypeValue("column3", ColumnType.BOOLEAN, null));

        List<StratioStream> streams = new ArrayList<>();
        StratioStream stream = new StratioStream(streamName, values, new ArrayList<StreamQuery>(),
                new HashSet<StreamAction>(), true);
        streams.add(stream);

        Mockito.when(ssaw.api().listStreams()).thenReturn(streams);

        CommandResult cr = shell.executeCommand("list");
        assertEquals(true, cr.isSuccess());
        assertEquals(getListResultFromName("listWithOneStream"), cr.getResult());
    }

    @Test
    public void listWithOneStreamOneQueryOneActionTest() throws StratioEngineStatusException,
            StratioAPISecurityException, StratioAPIGenericException, StratioEngineOperationException, IOException, StratioEngineConnectionException {
        String streamName = "testStream";
        List<ColumnNameTypeValue> values = new ArrayList<>();
        values.add(new ColumnNameTypeValue("column1", ColumnType.STRING, null));
        values.add(new ColumnNameTypeValue("column2", ColumnType.INTEGER, null));
        values.add(new ColumnNameTypeValue("column3", ColumnType.BOOLEAN, null));

        List<StreamQuery> querys = new ArrayList<>();
        querys.add(new StreamQuery("queryIdTest", "query raw test string"));

        Set<StreamAction> activeActions = new HashSet<>();
        activeActions.add(StreamAction.INDEXED);

        List<StratioStream> streams = new ArrayList<>();
        StratioStream stream = new StratioStream(streamName, values, querys, activeActions, true);
        streams.add(stream);

        Mockito.when(ssaw.api().listStreams()).thenReturn(streams);

        CommandResult cr = shell.executeCommand("list");
        assertEquals(true, cr.isSuccess());
        assertEquals(getListResultFromName("listWithOneStreamOneQueryOneAction"), cr.getResult());
    }
}
