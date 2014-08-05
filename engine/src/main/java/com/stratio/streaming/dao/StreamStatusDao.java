package com.stratio.streaming.dao;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.streaming.commons.constants.StreamAction;
import com.stratio.streaming.streams.QueryDTO;
import com.stratio.streaming.streams.StreamStatusDTO;

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

    public StreamStatusDTO addQuery(String streamName, String queryId, String queryRaw) {

        StreamStatusDTO streamStatus = streamStatuses.get(streamName);
        if (streamStatus != null) {
            streamStatus.getAddedQueries().put(queryId, new QueryDTO(queryRaw));
        } else {
            // TODO throw exception
        }

        return streamStatus;
    }

    public void removeQuery(String streamName, String queryId) {
        StreamStatusDTO streamStatus = streamStatuses.get(streamName);
        if (streamStatus != null) {
            streamStatus.getAddedQueries().remove(queryId);
        } else {
            // TODO throw exception
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
        return streamStatus.getActionsEnabled();
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
