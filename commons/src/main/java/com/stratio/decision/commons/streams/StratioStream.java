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
package com.stratio.decision.commons.streams;

import com.stratio.decision.commons.constants.StreamAction;
import com.stratio.decision.commons.messages.ColumnNameTypeValue;
import com.stratio.decision.commons.messages.StreamQuery;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

public class StratioStream implements Serializable {
    private String streamName;
    private List<ColumnNameTypeValue> columns;
    private List<StreamQuery> queries;
    private Set<StreamAction> activeActions;
    private boolean userDefined;

    public StratioStream(String streamName,
                         List<ColumnNameTypeValue> columns,
                         List<StreamQuery> queries,
                         Set<StreamAction> activeActions,
                         boolean userDefined) {
        this.streamName = streamName;
        this.columns = columns;
        this.queries = queries;
        this.userDefined = userDefined;
        this.activeActions = activeActions;
    }


    public boolean getUserDefined() {
        return userDefined;
    }

    public void setUserDefined(boolean userDefined) {
        this.userDefined = userDefined;
    }

    public List<StreamQuery> getQueries() {
        return queries;
    }

    public void setQueries(List<StreamQuery> queries) {
        this.queries = queries;
    }

    public String getStreamName() {
        return streamName;
    }

    public List<ColumnNameTypeValue> getColumns() {
        return columns;
    }

    public void setStreamName(String streamName) {
        this.streamName = streamName;
    }

    public void setColumns(List<ColumnNameTypeValue> columns) {
        this.columns = columns;
    }

    public Set<StreamAction> getActiveActions() {
        return activeActions;
    }

    public void setActiveActions(Set<StreamAction> activeActions) {
        this.activeActions = activeActions;
    }
}