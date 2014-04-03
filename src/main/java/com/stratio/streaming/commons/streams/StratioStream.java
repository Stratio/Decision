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
