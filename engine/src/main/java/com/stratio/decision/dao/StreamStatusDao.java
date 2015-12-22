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
package com.stratio.decision.dao;

import com.stratio.decision.commons.constants.EngineActionType;
import com.stratio.decision.commons.constants.StreamAction;
import com.stratio.decision.commons.messages.ColumnNameTypeValue;
import com.stratio.decision.streams.EngineActionDTO;
import com.stratio.decision.streams.QueryDTO;
import com.stratio.decision.streams.StreamStatusDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class StreamStatusDao {

    private static final Logger log = LoggerFactory.getLogger(StreamStatusDao.class);

    private final Map<String, StreamStatusDTO> streamStatuses;

    public StreamStatusDao() {
        streamStatuses = new ConcurrentHashMap<>();
    }

    public StreamStatusDTO createInferredStream(String streamName, List<ColumnNameTypeValue> columns) {
        StreamStatusDTO streamStatus = null;
        if (streamStatuses.get(streamName) == null) {
            streamStatus = new StreamStatusDTO(streamName, Boolean.FALSE, columns);
            streamStatuses.put(streamName, streamStatus);
        }

        return streamStatus;
    }

    public StreamStatusDTO create(String streamName, List<ColumnNameTypeValue> columns) {
        StreamStatusDTO streamStatus = new StreamStatusDTO(streamName, Boolean.TRUE, columns);

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

    public void enableEngineAction(String streamName, EngineActionType engineActionType, Object[] engineActionParams,
      String engineActionQueryId ){

        StreamStatusDTO streamStatus = streamStatuses.get(streamName);
        Map<EngineActionType, EngineActionDTO> engineActionsEnabled = streamStatus.getEngineActionsEnabled();

        EngineActionDTO engineActionDTO = new EngineActionDTO(engineActionType, engineActionParams,
                engineActionQueryId);

        engineActionsEnabled.put(engineActionType, engineActionDTO);

    }

    public void disableEngineAction(String streamName, EngineActionType engineActionType) {

        StreamStatusDTO streamStatus = streamStatuses.get(streamName);
        Map<EngineActionType, EngineActionDTO> engineActionsEnabled = streamStatus.getEngineActionsEnabled();

        if (engineActionsEnabled.containsKey(engineActionType)) {

            engineActionsEnabled.remove(engineActionType);
        }

    }

    public Boolean isEngineActionEnabled(String streamName, EngineActionType engineActionType) {

        StreamStatusDTO streamStatus = streamStatuses.get(streamName);
        Map<EngineActionType, EngineActionDTO> engineActionsEnabled = streamStatus.getEngineActionsEnabled();

        if (engineActionsEnabled.containsKey(engineActionType)) {

            return true;
        }

        return false;

    }

    public String getEngineActionQueryId(String streamName, EngineActionType engineActionType) {

        StreamStatusDTO streamStatus = streamStatuses.get(streamName);
        Map<EngineActionType, EngineActionDTO> engineActionsEnabled = streamStatus.getEngineActionsEnabled();

        if (engineActionsEnabled.containsKey(engineActionType)){

            return engineActionsEnabled.get(engineActionType).getEngineActionQueryId();

        }

        return null;

    }


    public void updateEngineActionParameters(String streamName, EngineActionType engineActionType, Object[]
            engineActionParams){

        StreamStatusDTO streamStatus = streamStatuses.get(streamName);
        Map<EngineActionType, EngineActionDTO> engineActionsEnabled = streamStatus.getEngineActionsEnabled();

        if (engineActionsEnabled.containsKey(engineActionType)){

            EngineActionDTO engineActionDTO = engineActionsEnabled.get(engineActionType);
            engineActionDTO.setEngineActionParameters(engineActionParams);

            engineActionsEnabled.put(engineActionType, engineActionDTO);
        }

    }

    public void addColumn(String streamName, ColumnNameTypeValue column){

        streamStatuses.get(streamName).getStreamDefinition().add(column);
        streamStatuses.get(streamName).getStreamColumns().put(column.getColumn(), column);

    }

    public Boolean existsColumnDefinition(String streamName, String columnName){

        if (streamStatuses.get(streamName).getStreamColumns().containsKey(columnName))
        {
            return true;
        }

        return false;
    }
}
