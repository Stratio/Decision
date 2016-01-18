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
package com.stratio.decision.streams;

import com.stratio.decision.commons.constants.EngineActionType;
import com.stratio.decision.commons.constants.StreamAction;
import com.stratio.decision.commons.messages.ColumnNameTypeValue;

import java.io.Serializable;
import java.util.*;

public class StreamStatusDTO implements Serializable {

    private static final long serialVersionUID = -714024710449331531L;

    private String streamName;
    private List<ColumnNameTypeValue> streamDefinition;
    private Map<String, ColumnNameTypeValue> streamColumns;
    private String actionQueryId;

    private Boolean userDefined;
    private final Set<StreamAction> actionsEnabled;
    private final Map<String, QueryDTO> addedQueries;

    private Map<EngineActionType, EngineActionDTO> engineActionsEnabled;

    public StreamStatusDTO(String streamName, Boolean userDefined, List<ColumnNameTypeValue> columns) {
        this.streamName = streamName;
        this.userDefined = userDefined;
        this.actionsEnabled = new HashSet<>();
        this.addedQueries = new HashMap<>();
        this.streamDefinition = columns!=null?columns:new ArrayList<>();
        this.engineActionsEnabled = new HashMap<>();

        this.streamColumns = new HashMap();
        streamDefinition.forEach( column -> streamColumns.put(column.getColumn(), column));

    }

    public String getStreamName() {
        return streamName;
    }

    public void setStreamName(String streamName) {
        this.streamName = streamName;
    }

    public List<ColumnNameTypeValue> getStreamDefinition() {
        return streamDefinition;
    }

    public void setStreamDefinition(List<ColumnNameTypeValue> streamDefinition) {
        this.streamDefinition = streamDefinition;
    }

    public Boolean getUserDefined() {
        return userDefined;
    }

    public void setUserDefined(Boolean userDefined) {
        this.userDefined = userDefined;
    }

    public String getActionQueryId() {
        return actionQueryId;
    }

    public void setActionQueryId(String actionQueryId) {
        this.actionQueryId = actionQueryId;
    }

    public Set<StreamAction> getActionsEnabled() {
        return actionsEnabled;
    }

    public Map<String, QueryDTO> getAddedQueries() {
        return addedQueries;
    }

    public Map<EngineActionType, EngineActionDTO> getEngineActionsEnabled() {
        return engineActionsEnabled;
    }

    public Map<String, ColumnNameTypeValue> getStreamColumns() {
        return streamColumns;
    }
}
