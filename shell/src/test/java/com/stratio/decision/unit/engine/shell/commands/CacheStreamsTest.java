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
package com.stratio.decision.unit.engine.shell.commands;

import com.stratio.decision.commons.constants.ColumnType;
import com.stratio.decision.commons.constants.StreamAction;
import com.stratio.decision.commons.exceptions.StratioAPIGenericException;
import com.stratio.decision.commons.exceptions.StratioEngineConnectionException;
import com.stratio.decision.commons.exceptions.StratioEngineStatusException;
import com.stratio.decision.commons.messages.ColumnNameTypeValue;
import com.stratio.decision.commons.messages.StreamQuery;
import com.stratio.decision.commons.streams.StratioStream;
import com.stratio.decision.shell.dao.CachedStreamsDAO;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.cache.CacheManager;
import org.springframework.cache.concurrent.ConcurrentMapCache;
import org.springframework.shell.core.CommandResult;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class CacheStreamsTest extends BaseShellTest {

    private static CacheManager cacheManager;

    private static CachedStreamsDAO cachedStreamsDAO;

    @Before
    public void setUp() {
        init();
        cacheManager = applicationContext.getBean(CacheManager.class);
        cachedStreamsDAO = applicationContext.getBean(CachedStreamsDAO.class);
    }

    @Test
    public void listOneElementCached() throws StratioEngineStatusException, StratioAPIGenericException, StratioEngineConnectionException {
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

        cachedStreamsDAO.listStreams();

        int cacheSize = ((ConcurrentMapCache) cacheManager.getCache("streams")).getNativeCache().size();

        assertEquals(1, cacheSize);

    }

    @Test
    public void listOneElementCachedAndRefreshed() throws StratioEngineStatusException, StratioAPIGenericException, StratioEngineConnectionException {
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

        cachedStreamsDAO.listStreams();

        CommandResult cr = shell.executeCommand("list");

        int cacheSize = ((ConcurrentMapCache) cacheManager.getCache("streams")).getNativeCache().size();

        assertEquals(true, cr.isSuccess());
        assertEquals(1, cacheSize);

    }
}
