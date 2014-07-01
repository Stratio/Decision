package com.stratio.streaming.shell.commands;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.cache.CacheManager;
import org.springframework.cache.concurrent.ConcurrentMapCache;
import org.springframework.shell.core.CommandResult;

import com.stratio.streaming.commons.constants.ColumnType;
import com.stratio.streaming.commons.constants.StreamAction;
import com.stratio.streaming.commons.exceptions.StratioAPIGenericException;
import com.stratio.streaming.commons.exceptions.StratioEngineStatusException;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.commons.messages.StreamQuery;
import com.stratio.streaming.commons.streams.StratioStream;
import com.stratio.streaming.shell.dao.CachedStreamsDAO;

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
    public void listOneElementCached() throws StratioEngineStatusException, StratioAPIGenericException {
        String streamName = "testStream";
        List<ColumnNameTypeValue> values = new ArrayList<>();
        values.add(new ColumnNameTypeValue("column1", ColumnType.STRING, null));
        values.add(new ColumnNameTypeValue("column2", ColumnType.INTEGER, null));
        values.add(new ColumnNameTypeValue("column3", ColumnType.BOOLEAN, null));

        List<StratioStream> streams = new ArrayList<>();
        StratioStream stream = new StratioStream(streamName, values, new ArrayList<StreamQuery>(),
                new HashSet<StreamAction>(), true);
        streams.add(stream);

        Mockito.when(stratioStreamingApi.listStreams()).thenReturn(streams);

        cachedStreamsDAO.listStreams();

        int cacheSize = ((ConcurrentMapCache) cacheManager.getCache("streams")).getNativeCache().size();

        assertEquals(1, cacheSize);

    }

    @Test
    public void listOneElementCachedAndRefreshed() throws StratioEngineStatusException, StratioAPIGenericException {
        String streamName = "testStream";
        List<ColumnNameTypeValue> values = new ArrayList<>();
        values.add(new ColumnNameTypeValue("column1", ColumnType.STRING, null));
        values.add(new ColumnNameTypeValue("column2", ColumnType.INTEGER, null));
        values.add(new ColumnNameTypeValue("column3", ColumnType.BOOLEAN, null));

        List<StratioStream> streams = new ArrayList<>();
        StratioStream stream = new StratioStream(streamName, values, new ArrayList<StreamQuery>(),
                new HashSet<StreamAction>(), true);
        streams.add(stream);

        Mockito.when(stratioStreamingApi.listStreams()).thenReturn(streams);

        cachedStreamsDAO.listStreams();

        CommandResult cr = shell.executeCommand("list");

        int cacheSize = ((ConcurrentMapCache) cacheManager.getCache("streams")).getNativeCache().size();

        assertEquals(true, cr.isSuccess());
        assertEquals(1, cacheSize);

    }
}
