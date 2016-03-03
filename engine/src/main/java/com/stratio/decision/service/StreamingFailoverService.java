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
package com.stratio.decision.service;

import com.stratio.decision.commons.constants.EngineActionType;
import com.stratio.decision.commons.constants.StreamAction;
import com.stratio.decision.dao.StreamStatusDao;
import com.stratio.decision.dao.StreamingFailoverDao;
import com.stratio.decision.model.FailoverPersistenceStoreModel;
import com.stratio.decision.streams.EngineActionDTO;
import com.stratio.decision.streams.QueryDTO;
import com.stratio.decision.streams.StreamStatusDTO;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Map;

public class StreamingFailoverService {

    private final StreamStatusDao streamStatusDao;
    private final StreamMetadataService streamMetadataService;
    private final StreamingFailoverDao streamingFailoverDao;

    @Autowired
    private StreamOperationService streamOperationService;


    public StreamingFailoverService(StreamStatusDao streamStatusDao, StreamMetadataService streamMetadataService,
                                    StreamingFailoverDao streamingFailoverDao) {
        this.streamStatusDao = streamStatusDao;
        this.streamMetadataService = streamMetadataService;
        this.streamingFailoverDao = streamingFailoverDao;
    }

    public synchronized void load() throws Exception {
        FailoverPersistenceStoreModel failoverPersistenceStoreModel = streamingFailoverDao.load();
        if (failoverPersistenceStoreModel != null) {
            streamStatusDao.putAll(failoverPersistenceStoreModel.getStreamStatuses());
            Map<String, StreamStatusDTO> streamsStatus = failoverPersistenceStoreModel.getStreamStatuses();
            for (Map.Entry<String, StreamStatusDTO> entry : streamsStatus.entrySet()) {
                StreamStatusDTO stream = entry.getValue();
                streamOperationService.createStream(stream.getStreamName(), stream.getStreamDefinition());
                for (Map.Entry<String, QueryDTO> query : stream.getAddedQueries().entrySet()) {
                    streamOperationService.addQuery(entry.getKey(), query.getValue().getQueryRaw());
                }
                for (StreamAction action : stream.getActionsEnabled()) {
                    streamOperationService.enableAction(entry.getKey(), action);
                }

                // Enable engine actions
                for (Map.Entry<EngineActionType, EngineActionDTO> engineAction : stream.getEngineActionsEnabled()
                        .entrySet()) {

                    streamOperationService.enableEngineAction(stream.getStreamName(), engineAction.getKey(),
                            engineAction.getValue().getEngineActionParameters());
                }


            }
//            streamMetadataService.setSnapshot(failoverPersistenceStoreModel.getSiddhiSnapshot());
        }
    }

    public synchronized void save() throws Exception {
//        streamingFailoverDao.save(new FailoverPersistenceStoreModel(streamStatusDao.getAll(), streamMetadataService
//                .getSnapshot()));
        streamingFailoverDao.save(new FailoverPersistenceStoreModel(streamStatusDao.getAll(), null));
    }

}
