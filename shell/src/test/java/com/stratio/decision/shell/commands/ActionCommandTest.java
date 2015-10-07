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

import com.stratio.decision.commons.exceptions.StratioAPIGenericException;
import com.stratio.decision.commons.exceptions.StratioEngineConnectionException;
import com.stratio.decision.commons.exceptions.StratioEngineStatusException;
import com.stratio.decision.commons.streams.StratioStream;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.shell.core.CommandResult;

import java.io.IOException;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class ActionCommandTest extends BaseShellTest {

    @Before
    public void setUp() {
        init();
    }

    @Test
    public void indexStreamStartTest() throws StratioEngineStatusException, StratioAPIGenericException, IOException,
            StratioEngineConnectionException {
        Mockito.when(ssaw.api().listStreams()).thenReturn(new ArrayList<StratioStream>());
        CommandResult cr = shell.executeCommand("index start --stream testStream");
        assertEquals(true, cr.isSuccess());
        assertEquals("Stream testStream indexed correctly", cr.getResult());
    }

    @Test
    public void indexStreamStopTest() throws StratioEngineStatusException, StratioAPIGenericException, IOException,
            StratioEngineConnectionException {
        Mockito.when(ssaw.api().listStreams()).thenReturn(new ArrayList<StratioStream>());
        CommandResult cr = shell.executeCommand("index stop --stream testStream");
        assertEquals(true, cr.isSuccess());
        assertEquals("Stream testStream unindexed correctly", cr.getResult());
    }

    @Test
    public void listenStreamStartTest() throws StratioEngineStatusException, StratioAPIGenericException, IOException,
            StratioEngineConnectionException {
        Mockito.when(ssaw.api().listStreams()).thenReturn(new ArrayList<StratioStream>());
        CommandResult cr = shell.executeCommand("listen start --stream testStream");
        assertEquals(true, cr.isSuccess());
        assertEquals("Stream testStream listened correctly", cr.getResult());
    }

    @Test
    public void listenStreamStopTest() throws StratioEngineStatusException, StratioAPIGenericException, IOException,
            StratioEngineConnectionException {
        Mockito.when(ssaw.api().listStreams()).thenReturn(new ArrayList<StratioStream>());
        CommandResult cr = shell.executeCommand("listen stop --stream testStream");
        assertEquals(true, cr.isSuccess());
        assertEquals("Stream testStream unlistened correctly", cr.getResult());
    }

    @Test
    public void saveCassandraStreamStartTest() throws StratioEngineStatusException, StratioAPIGenericException,
            IOException,
            StratioEngineConnectionException {
        Mockito.when(ssaw.api().listStreams()).thenReturn(new ArrayList<StratioStream>());
        CommandResult cr = shell.executeCommand("save cassandra start --stream testStream");
        assertEquals(true, cr.isSuccess());
        assertEquals("Stream testStream attached to cassandra correctly", cr.getResult());
    }

    @Test
    public void saveCassandraStreamStopTest() throws StratioEngineStatusException, StratioAPIGenericException,
            IOException,
            StratioEngineConnectionException {
        Mockito.when(ssaw.api().listStreams()).thenReturn(new ArrayList<StratioStream>());
        CommandResult cr = shell.executeCommand("save cassandra stop --stream testStream");
        assertEquals(true, cr.isSuccess());
        assertEquals("Stream testStream de-attached from cassandra correctly", cr.getResult());
    }

    @Test
    public void saveMongoStreamStartTest() throws StratioEngineStatusException, StratioAPIGenericException,
            IOException,
            StratioEngineConnectionException {
        Mockito.when(ssaw.api().listStreams()).thenReturn(new ArrayList<StratioStream>());
        CommandResult cr = shell.executeCommand("save mongo start --stream testStream");
        assertEquals(true, cr.isSuccess());
        assertEquals("Stream testStream attached to mongo correctly", cr.getResult());
    }

    @Test
    public void saveMongoStreamStopTest() throws StratioEngineStatusException, StratioAPIGenericException,
            IOException,
            StratioEngineConnectionException {
        Mockito.when(ssaw.api().listStreams()).thenReturn(new ArrayList<StratioStream>());
        CommandResult cr = shell.executeCommand("save mongo stop --stream testStream");
        assertEquals(true, cr.isSuccess());
        assertEquals("Stream testStream de-attached from mongo correctly", cr.getResult());
    }

    @Test
    public void saveSolrStreamStartTest() throws StratioEngineStatusException, StratioAPIGenericException,
            IOException,
            StratioEngineConnectionException {
        Mockito.when(ssaw.api().listStreams()).thenReturn(new ArrayList<StratioStream>());
        CommandResult cr = shell.executeCommand("save solr start --stream testStream");
        assertEquals(true, cr.isSuccess());
        assertEquals("Stream testStream attached to solr correctly", cr.getResult());
    }

    @Test
    public void saveSolrStreamStopTest() throws StratioEngineStatusException, StratioAPIGenericException,
            IOException,
            StratioEngineConnectionException {
        Mockito.when(ssaw.api().listStreams()).thenReturn(new ArrayList<StratioStream>());
        CommandResult cr = shell.executeCommand("save solr stop --stream testStream");
        assertEquals(true, cr.isSuccess());
        assertEquals("Stream testStream de-attached from solr correctly", cr.getResult());
    }

}
