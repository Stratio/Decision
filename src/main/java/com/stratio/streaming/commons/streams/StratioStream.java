/*******************************************************************************
 * Copyright 2014 Stratio
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.stratio.streaming.commons.streams;

import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.commons.messages.StreamQuery;

import java.io.Serializable;
import java.util.List;

public class StratioStream implements Serializable {
    private String streamName;
    private List<ColumnNameTypeValue> columns;
    private List<StreamQuery> queries;
    private boolean userDefined;

    public StratioStream(String streamName,
                         List<ColumnNameTypeValue> columns,
                         List<StreamQuery> queries,
                         boolean userDefined) {
        this.streamName = streamName;
        this.columns = columns;
        this.queries = queries;
        this.userDefined = userDefined;
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
}
