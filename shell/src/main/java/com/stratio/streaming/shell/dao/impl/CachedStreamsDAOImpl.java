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
package com.stratio.streaming.shell.dao.impl;

import java.util.List;

import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.Caching;

import com.stratio.streaming.api.IStratioStreamingAPI;
import com.stratio.streaming.api.messaging.ColumnNameType;
import com.stratio.streaming.commons.exceptions.StratioAPIGenericException;
import com.stratio.streaming.commons.exceptions.StratioAPISecurityException;
import com.stratio.streaming.commons.exceptions.StratioEngineOperationException;
import com.stratio.streaming.commons.exceptions.StratioEngineStatusException;
import com.stratio.streaming.commons.streams.StratioStream;
import com.stratio.streaming.shell.dao.CachedStreamsDAO;

public class CachedStreamsDAOImpl implements CachedStreamsDAO {

    private final IStratioStreamingAPI stratioStreamingApi;

    public CachedStreamsDAOImpl(IStratioStreamingAPI stratioStreamingApi) {
        this.stratioStreamingApi = stratioStreamingApi;
    }

    @Cacheable(value = "streams")
    @Override
    public List<StratioStream> listStreams() throws StratioEngineStatusException, StratioAPIGenericException {
        return stratioStreamingApi.listStreams();
    }

    @Override
    @Caching(evict = @CacheEvict(value = "streams", allEntries = true, beforeInvocation = true), cacheable = @Cacheable(value = "streams"))
    public List<StratioStream> listUncachedStreams() throws StratioEngineStatusException, StratioAPIGenericException {
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
