package com.stratio.streaming.shell.dao.impl;

import java.util.List;

import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;

import com.stratio.streaming.api.IStratioStreamingAPI;
import com.stratio.streaming.commons.exceptions.StratioAPISecurityException;
import com.stratio.streaming.commons.exceptions.StratioEngineOperationException;
import com.stratio.streaming.commons.exceptions.StratioEngineStatusException;
import com.stratio.streaming.commons.streams.StratioStream;
import com.stratio.streaming.messaging.ColumnNameType;
import com.stratio.streaming.shell.dao.CachedStreamsDAO;

public class CachedStreamsDAOImpl implements CachedStreamsDAO {

    private final IStratioStreamingAPI stratioStreamingApi;

    public CachedStreamsDAOImpl(IStratioStreamingAPI stratioStreamingApi) {
        this.stratioStreamingApi = stratioStreamingApi;
    }

    @Cacheable(value = "streams")
    @Override
    public List<StratioStream> listStreams() throws StratioEngineStatusException {
        return stratioStreamingApi.listStreams();
    }

    @Override
    @CacheEvict(value = "streams", allEntries = true)
    public List<StratioStream> listUncachedStreams() throws StratioEngineStatusException {
        return stratioStreamingApi.listStreams();
    }

    @CacheEvict(value = "streams", allEntries = true)
    @Override
    public void newStream(String name, List<ColumnNameType> columns) throws StratioEngineStatusException,
            StratioAPISecurityException, StratioEngineOperationException {
        stratioStreamingApi.createStream(name, columns);
    }

    @CacheEvict(value = "streams", allEntries = true)
    @Override
    public void dropStream(String name) throws StratioEngineStatusException, StratioAPISecurityException,
            StratioEngineOperationException {
        stratioStreamingApi.dropStream(name);
    }
}
