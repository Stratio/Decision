/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 * <promptProvider>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <promptProvider>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <promptProvider>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.decision.shell.commands;

import com.stratio.decision.commons.constants.ColumnType;
import com.stratio.decision.commons.constants.StreamAction;
import com.stratio.decision.commons.exceptions.*;
import com.stratio.decision.commons.messages.ColumnNameTypeValue;
import com.stratio.decision.commons.messages.StreamQuery;
import com.stratio.decision.commons.streams.StratioStream;
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
    public void listWith0StreamsTest() throws StratioEngineStatusException, StratioAPIGenericException, IOException,
            StratioEngineConnectionException, StratioEngineOperationException {
        Mockito.when(ssaw.api().listStreams()).thenReturn(new ArrayList<StratioStream>());
        CommandResult cr = shell.executeCommand("list");
        assertEquals(true, cr.isSuccess());
        assertEquals(getListResultFromName("listWithNoStreams"), cr.getResult());
    }

    @Test
    public void listWithOneStreamTest() throws StratioEngineStatusException, StratioAPIGenericException,
            StratioAPISecurityException, StratioEngineOperationException, IOException,
            StratioEngineConnectionException {
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
            StratioAPISecurityException, StratioAPIGenericException, StratioEngineOperationException, IOException,
            StratioEngineConnectionException {
        String streamName = "testStream";
        List<ColumnNameTypeValue> values = new ArrayList<>();
        values.add(new ColumnNameTypeValue("column1", ColumnType.STRING, null));
        values.add(new ColumnNameTypeValue("column2", ColumnType.INTEGER, null));
        values.add(new ColumnNameTypeValue("column3", ColumnType.BOOLEAN, null));

        List<StreamQuery> querys = new ArrayList<>();
        querys.add(new StreamQuery("queryIdTest", "query raw test string"));

        Set<StreamAction> activeActions = new HashSet<>();
        activeActions.add(StreamAction.SAVE_TO_ELASTICSEARCH);

        List<StratioStream> streams = new ArrayList<>();
        StratioStream stream = new StratioStream(streamName, values, querys, activeActions, true);
        streams.add(stream);

        Mockito.when(ssaw.api().listStreams()).thenReturn(streams);

        CommandResult cr = shell.executeCommand("list");
        assertEquals(true, cr.isSuccess());
        assertEquals(getListResultFromName("listWithOneStreamOneQueryOneAction"), cr.getResult());
    }

    @Test
    public void columnsStreamTest() throws StratioEngineStatusException,
            StratioAPISecurityException, StratioAPIGenericException, StratioEngineOperationException, IOException,
            StratioEngineConnectionException {

        String streamName = "testStream";
        List<ColumnNameTypeValue> values = new ArrayList<>();
        values.add(new ColumnNameTypeValue("column1", ColumnType.STRING, null));
        values.add(new ColumnNameTypeValue("column2", ColumnType.INTEGER, null));
        values.add(new ColumnNameTypeValue("column3", ColumnType.BOOLEAN, null));

        List<StreamQuery> querys = new ArrayList<>();
        querys.add(new StreamQuery("queryIdTest", "query raw test string"));

        Set<StreamAction> activeActions = new HashSet<>();
        activeActions.add(StreamAction.SAVE_TO_ELASTICSEARCH);

        List<StratioStream> streams = new ArrayList<>();
        StratioStream stream = new StratioStream(streamName, values, querys, activeActions, true);
        streams.add(stream);

        Mockito.when(ssaw.api().columnsFromStream("testStream")).thenReturn(values);

        CommandResult cr2 = shell.executeCommand("columns --stream testStream");

        assertEquals(true, cr2.isSuccess());
        assertEquals(getListResultFromName("columnsStream"), cr2.getResult());

    }

    @Test
    public void createStreamTest() throws StratioEngineStatusException,
            StratioAPISecurityException, StratioAPIGenericException, StratioEngineOperationException, IOException,
            StratioEngineConnectionException {

        CommandResult cr = shell.executeCommand("create --stream testStream --definition column1.string,column2"
                + ".integer,column3.boolean");
        assertEquals(true, cr.isSuccess());
        assertEquals("Stream testStream created correctly", cr.getResult());

    }

    @Test
    public void dropStreamTest() throws StratioEngineStatusException,
            StratioAPISecurityException, StratioAPIGenericException, StratioEngineOperationException, IOException,
            StratioEngineConnectionException {

        CommandResult cr2 = shell.executeCommand("drop --stream testStream");

        assertEquals(true, cr2.isSuccess());
        assertEquals("Stream testStream dropped correctly", cr2.getResult());

    }

    @Test
    public void alterStreamTest() throws StratioEngineStatusException,
            StratioAPISecurityException, StratioAPIGenericException, StratioEngineOperationException, IOException,
            StratioEngineConnectionException {

        CommandResult cr = shell.executeCommand("alter --stream testStream --definition name.long");

        assertEquals(true, cr.isSuccess());
        assertEquals("Stream testStream altered correctly", cr.getResult());

    }

    @Test
    public void insertStreamTest() throws StratioEngineStatusException,
            StratioAPISecurityException, StratioAPIGenericException, StratioEngineOperationException, IOException,
            StratioEngineConnectionException {

        CommandResult cr = shell.executeCommand("insert --stream testStream --values name.Name");

        assertEquals(true, cr.isSuccess());
        assertEquals("Added an event to stream testStream correctly", cr.getResult());

    }

    @Test
    public void removeQueryTest() throws StratioEngineStatusException,
            StratioAPISecurityException, StratioAPIGenericException, StratioEngineOperationException, IOException,
            StratioEngineConnectionException {
        String streamName = "testStream";
        List<ColumnNameTypeValue> values = new ArrayList<>();
        values.add(new ColumnNameTypeValue("column1", ColumnType.STRING, null));
        values.add(new ColumnNameTypeValue("column2", ColumnType.INTEGER, null));
        values.add(new ColumnNameTypeValue("column3", ColumnType.BOOLEAN, null));

        List<StreamQuery> querys = new ArrayList<>();
        querys.add(new StreamQuery("queryIdTest", "query raw test string"));

        Set<StreamAction> activeActions = new HashSet<>();
        activeActions.add(StreamAction.SAVE_TO_ELASTICSEARCH);

        List<StratioStream> streams = new ArrayList<>();
        StratioStream stream = new StratioStream(streamName, values, querys, activeActions, true);
        streams.add(stream);

        Mockito.when(ssaw.api().listStreams()).thenReturn(streams);

        CommandResult cr = shell.executeCommand("remove query --stream testStream --id queryIdTest");

        assertEquals(true, cr.isSuccess());
        assertEquals("Query testStream dropped correctly with id queryIdTest", cr.getResult());

    }
}
