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
package com.stratio.streaming.service;

import com.stratio.streaming.dao.StreamStatusDao;
import com.stratio.streaming.dao.StreamingFailoverDao;
import com.stratio.streaming.exception.CacheException;
import com.stratio.streaming.model.CassandraPersistenceStoreModel;

public class StreamingFailoverService {

    private final StreamStatusDao streamStatusDao;
    private final StreamMetadataService streamMetadataService;
    private final StreamingFailoverDao streamingFailoverDao;

    public StreamingFailoverService(StreamStatusDao streamStatusDao, StreamMetadataService streamMetadataService,
            StreamingFailoverDao streamingFailoverDao) {
        this.streamStatusDao = streamStatusDao;
        this.streamMetadataService = streamMetadataService;
        this.streamingFailoverDao = streamingFailoverDao;
    }

    public synchronized void load() throws CacheException {
        CassandraPersistenceStoreModel cassandraPersistenceStoreModel = streamingFailoverDao.load();
        if (cassandraPersistenceStoreModel != null) {
            streamStatusDao.putAll(cassandraPersistenceStoreModel.getStreamStatuses());
            streamMetadataService.setSnapshot(cassandraPersistenceStoreModel.getSiddhiSnapshot());
        }
    }

    public synchronized void save() {
        streamingFailoverDao.save(new CassandraPersistenceStoreModel(streamStatusDao.getAll(), streamMetadataService
                .getSnapshot()));
    }

}
