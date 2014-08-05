package com.stratio.streaming.service;

import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.query.api.definition.Attribute;

public class StreamMetadataService {

    private final SiddhiManager siddhiManager;

    public StreamMetadataService(SiddhiManager siddhiManager) {
        this.siddhiManager = siddhiManager;
    }

    public int getAttributePosition(String streamName, String columnName) {
        return siddhiManager.getStreamDefinition(streamName).getAttributePosition(columnName);
    }

    public Attribute getAttribute(String streamName, int columnOrder) {
        return siddhiManager.getStreamDefinition(streamName).getAttributeList().get(columnOrder);
    }
}
