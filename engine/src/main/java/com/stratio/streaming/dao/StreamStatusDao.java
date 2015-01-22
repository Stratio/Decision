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
import com.stratio.streaming.streams.QueryDTO;
import com.stratio.streaming.streams.StreamStatusDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class StreamStatusDao {

    private static final Logger log = LoggerFactory.getLogger(StreamStatusDao.class);

    private final Map<String, StreamStatusDTO> streamStatuses;

    public StreamStatusDao() {
        streamStatuses = new ConcurrentHashMap<>();
    }

    public StreamStatusDTO createInferredStream(String streamName) {
        StreamStatusDTO streamStatus = null;
        if (streamStatuses.get(streamName) == null) {
            streamStatus = new StreamStatusDTO(streamName, Boolean.FALSE);
            streamStatuses.put(streamName, streamStatus);
        }

        return streamStatus;
    }

    public StreamStatusDTO create(String streamName) {
        StreamStatusDTO streamStatus = new StreamStatusDTO(streamName, Boolean.TRUE);

        if (streamStatuses.put(streamName, streamStatus) != null) {
            log.warn("Stream status of stream {} has been updated", streamName);
        }

        return streamStatus;
    }

    public void remove(String streamName) {
        streamStatuses.remove(streamName);
    }

    public StreamStatusDTO get(String streamName) {
        return streamStatuses.get(streamName);
    }

    public Map<String, StreamStatusDTO> getAll() {
        return streamStatuses;
    }

    public void putAll(Map<String, StreamStatusDTO> streamStatuses) {
        this.streamStatuses.putAll(streamStatuses);
    }

    public StreamStatusDTO addQuery(String streamName, String queryId, String queryRaw) {
        StreamStatusDTO streamStatus = streamStatuses.get(streamName);
        if (streamStatus != null) {
            streamStatus.getAddedQueries().put(queryId, new QueryDTO(queryRaw));
        }
        return streamStatus;
    }

    public void removeQuery(String streamName, String queryId) {
        StreamStatusDTO streamStatus = streamStatuses.get(streamName);
        if (streamStatus != null) {
            streamStatus.getAddedQueries().remove(queryId);
        }
    }

    public void enableAction(String streamName, StreamAction action) {
        StreamStatusDTO streamStatus = streamStatuses.get(streamName);
        streamStatus.getActionsEnabled().add(action);
    }

    public void disableAction(String streamName, StreamAction action) {
        StreamStatusDTO streamStatus = streamStatuses.get(streamName);
        streamStatus.getActionsEnabled().remove(action);
    }

    public Set<StreamAction> getEnabledActions(String streamName) {
        StreamStatusDTO streamStatus = streamStatuses.get(streamName);
        Set<StreamAction> enabledActions = new HashSet<>();
        if (streamStatus != null) {
            enabledActions = streamStatus.getActionsEnabled();
        }
        return enabledActions;
    }

    public String getActionQuery(String streamName) {
        StreamStatusDTO streamStatus = streamStatuses.get(streamName);
        String actionQueryId = streamStatus.getActionQueryId();
        streamStatus.setActionQueryId(null);
        return actionQueryId;
    }

    public void setActionQuery(String streamName, String queryId) {
        StreamStatusDTO streamStatus = streamStatuses.get(streamName);
        streamStatus.setActionQueryId(queryId);
    }
}
