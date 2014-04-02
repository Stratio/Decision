package com.stratio.streaming.commons.streams;

import com.stratio.streaming.commons.messages.ColumnNameTypeValue;

import java.util.List;

public class StratioStream {
    private String streamName;
    private List<ColumnNameTypeValue> columns;

    public StratioStream(String streamName, List<ColumnNameTypeValue> columns) {
        this.streamName = streamName;
        this.columns = columns;
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
