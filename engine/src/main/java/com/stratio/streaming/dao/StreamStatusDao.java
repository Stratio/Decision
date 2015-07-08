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
package com.stratio.streaming.dao;

import com.stratio.streaming.commons.constants.StreamAction;
import com.stratio.streaming.configuration.StreamingStatusDaoConfiguration;
import com.stratio.streaming.exception.CacheException;
import com.stratio.streaming.streams.QueryDTO;
import com.stratio.streaming.streams.StreamStatusDTO;
import com.stratio.streaming.utils.Cache;
import com.stratio.streaming.utils.hazelcast.HazelcastCache;
import com.stratio.streaming.utils.hazelcast.StreamingHazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Component
@Import({StreamingStatusDaoConfiguration.class})
public class StreamStatusDao {

    private static final Logger log = LoggerFactory.getLogger(StreamStatusDao.class);

    @Autowired
    private StreamingHazelcastInstance streamingHazelcastInstance;

    private Cache<String, StreamStatusDTO> streamStatuses;


    public StreamStatusDao() {
        streamStatuses = new HazelcastCache<>(streamingHazelcastInstance.getHazelcastInstance(), "streamStatuses");
    }

    public StreamStatusDTO createInferredStream(String streamName) throws CacheException {
        StreamStatusDTO streamStatus = null;
        if (streamStatuses.get(streamName) == null) {
            streamStatus = new StreamStatusDTO(streamName, Boolean.FALSE);
            streamStatuses.put(streamName, streamStatus);
        }

        return streamStatus;
    }

    public StreamStatusDTO create(String streamName) throws  CacheException {
        StreamStatusDTO streamStatus = new StreamStatusDTO(streamName, Boolean.TRUE);

        if (streamStatuses.put(streamName, streamStatus) != null) {
            log.warn("Stream status of stream {} has been updated", streamName);
        }

        return streamStatus;
    }

    public void remove(String streamName) throws CacheException {
        streamStatuses.remove(streamName);
    }

    public StreamStatusDTO get(String streamName) throws CacheException {
        return streamStatuses.get(streamName);
    }

    public Map<String, StreamStatusDTO> getAll() {
        return streamStatuses.asMap();
    }

    public void putAll(Map<String, StreamStatusDTO> streamStatuses) throws CacheException {
        this.streamStatuses.putAll(streamStatuses);
    }

    public StreamStatusDTO addQuery(String streamName, String queryId, String queryRaw) throws CacheException {
        StreamStatusDTO streamStatus = streamStatuses.get(streamName);
        if (streamStatus != null) {
            streamStatus.getAddedQueries().put(queryId, new QueryDTO(queryRaw));
        }
        return streamStatus;
    }

    public void removeQuery(String streamName, String queryId) throws CacheException {
        StreamStatusDTO streamStatus = streamStatuses.get(streamName);
        if (streamStatus != null) {
            streamStatus.getAddedQueries().remove(queryId);
        }
    }

    public void enableAction(String streamName, StreamAction action) throws CacheException {
        StreamStatusDTO streamStatus = streamStatuses.get(streamName);
        streamStatus.getActionsEnabled().add(action);
    }

    public void disableAction(String streamName, StreamAction action) throws CacheException {
        StreamStatusDTO streamStatus = streamStatuses.get(streamName);
        streamStatus.getActionsEnabled().remove(action);
    }

    public Set<StreamAction> getEnabledActions(String streamName) throws CacheException {
        StreamStatusDTO streamStatus = streamStatuses.get(streamName);
        Set<StreamAction> enabledActions = new HashSet<>();
        if (streamStatus != null) {
            enabledActions = streamStatus.getActionsEnabled();
        }
        return enabledActions;
    }

    public String getActionQuery(String streamName) throws CacheException {
        StreamStatusDTO streamStatus = streamStatuses.get(streamName);
        String actionQueryId = streamStatus.getActionQueryId();
        streamStatus.setActionQueryId(null);
        return actionQueryId;
    }

    public void setActionQuery(String streamName, String queryId) throws CacheException {
        StreamStatusDTO streamStatus = streamStatuses.get(streamName);
        streamStatus.setActionQueryId(queryId);
    }

}
