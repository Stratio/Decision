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
package com.stratio.streaming.commons.messages;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.stratio.streaming.commons.constants.StreamAction;

public class StratioStreamingMessage implements Serializable {

    private static final long serialVersionUID = -3259551728685320551L;

    private String operation;
    private String streamName;
    private String session_id;
    private String request_id;
    private String request;
    private Long timestamp;
    private List<ColumnNameTypeValue> columns;
    private final List<StreamQuery> queries;
    private Set<StreamAction> activeActions;
    private Boolean userDefined;

    public StratioStreamingMessage() {
        this.queries = new ArrayList<>();
    }

    /**
     * Used in the API MessageBuilder
     * 
     * @param operation
     * @param streamName
     * @param sessionId
     * @param requestId
     * @param request
     * @param timeStamp
     * @param columns
     * @param queries
     * @param userDefined
     */
    public StratioStreamingMessage(String operation, String streamName, String sessionId, String requestId,
            String request, Long timeStamp, List<ColumnNameTypeValue> columns, List<StreamQuery> queries,
            Boolean userDefined) {
        this.operation = operation;
        this.streamName = streamName;
        this.session_id = sessionId;
        this.request_id = requestId;
        this.request = request;
        this.timestamp = timeStamp;
        this.columns = columns;
        this.queries = queries;
        this.userDefined = userDefined;
    }

    /**
     * Used in List Operation
     * 
     * @param streamName
     * @param columns
     */
    public StratioStreamingMessage(String streamName, List<ColumnNameTypeValue> columns, List<StreamQuery> queries) {
        this.streamName = streamName;
        this.columns = columns;
        this.queries = queries;
    }

    /**
     * Used in events output
     * 
     * @param streamName
     * @param timestamp
     * @param columns
     */
    public StratioStreamingMessage(String streamName, Long timestamp, List<ColumnNameTypeValue> columns) {
        this.queries = new ArrayList<>();
        this.streamName = streamName;
        this.timestamp = timestamp;
        this.columns = columns;
    }

    public void addColumn(ColumnNameTypeValue column) {
        if (columns == null) {
            columns = Lists.newArrayList();
        }
        this.columns.add(column);
    }

    public void addQuery(StreamQuery query) {
        this.queries.add(query);
    }

    public String getRequest_id() {
        return request_id;
    }

    public void setRequest_id(String request_id) {
        this.request_id = request_id;
    }

    public Boolean isUserDefined() {
        return userDefined;
    }

    public void setUserDefined(Boolean userDefined) {
        this.userDefined = userDefined;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public String getStreamName() {
        return streamName;
    }

    public void setStreamName(String streamName) {
        this.streamName = streamName;
    }

    public String getSession_id() {
        return session_id;
    }

    public void setSession_id(String session_id) {
        this.session_id = session_id;
    }

    public String getRequest() {
        return request;
    }

    public void setRequest(String request) {
        this.request = request;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public List<ColumnNameTypeValue> getColumns() {
        return columns;
    }

    public void setColumns(List<ColumnNameTypeValue> columns) {
        this.columns = columns;
    }

    public List<StreamQuery> getQueries() {
        return queries;
    }

    public Set<StreamAction> getActiveActions() {
        return activeActions != null ? activeActions : new HashSet<StreamAction>();
    }

    public void setActiveActions(Set<StreamAction> activeActions) {
        this.activeActions = activeActions;
    }
}
