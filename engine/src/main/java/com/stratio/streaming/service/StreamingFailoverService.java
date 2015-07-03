/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.streaming.service;

import com.stratio.streaming.dao.StreamStatusDao;
import com.stratio.streaming.dao.StreamingFailoverDao;
import com.stratio.streaming.model.CassandraPersistenceStoreModel;
import com.stratio.streaming.streams.StreamStatusDTO;
import java.util.Map;

public class StreamingFailoverService {

    private final StreamStatusDao streamStatusDao;
    private final StreamMetadataService streamMetadataService;
    private final StreamingFailoverDao streamingFailoverDao;
    private final StreamOperationService streamOperationService;

    public StreamingFailoverService(StreamStatusDao streamStatusDao, StreamMetadataService streamMetadataService,
                                    StreamingFailoverDao streamingFailoverDao, StreamOperationService streamOperationService) {
        this.streamStatusDao = streamStatusDao;
        this.streamMetadataService = streamMetadataService;
        this.streamingFailoverDao = streamingFailoverDao;
        this.streamOperationService = streamOperationService;
    }

    public synchronized void load() {
        CassandraPersistenceStoreModel cassandraPersistenceStoreModel = streamingFailoverDao.load();
        if (cassandraPersistenceStoreModel != null) {
            Map<String, StreamStatusDTO> streamsStatus = cassandraPersistenceStoreModel.getStreamStatuses();
            streamStatusDao.putAll(cassandraPersistenceStoreModel.getStreamStatuses());
            for (Map.Entry<String, StreamStatusDTO> entry : streamsStatus.entrySet()) {
                StreamStatusDTO streamStatusDTO = entry.getValue();
                streamOperationService.createStream(streamStatusDTO.getStreamName(), streamStatusDTO.getStreamDefinition());
            }
            streamMetadataService.setSnapshot(cassandraPersistenceStoreModel.getSiddhiSnapshot());
        }
    }

    public synchronized void save() {
        streamingFailoverDao.save(new CassandraPersistenceStoreModel(streamStatusDao.getAll(), streamMetadataService
                .getSnapshot()));
    }

}
