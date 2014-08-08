package com.stratio.streaming.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.ryantenney.metrics.annotation.CachedGauge;
import com.stratio.streaming.dao.StreamStatusDao;
import com.stratio.streaming.streams.StreamStatusDTO;

public class StreamStatusMetricService {
    private final StreamStatusDao streamStatusDao;

    public StreamStatusMetricService(StreamStatusDao streamStatusDao) {
        this.streamStatusDao = streamStatusDao;
    }

    @CachedGauge(absolute = true, name = "streams.count", timeout = 30, timeoutUnit = TimeUnit.SECONDS)
    public int getTotalStreams() {
        return streamStatusDao.getAll().size();
    }

    @CachedGauge(absolute = true, name = "streams.names", timeout = 30, timeoutUnit = TimeUnit.SECONDS)
    public List<String> getStreamNames() {
        Map<String, StreamStatusDTO> streams = streamStatusDao.getAll();
        List<String> result = new ArrayList<>();
        for (StreamStatusDTO stream : streams.values()) {
            result.add(stream.getStreamName());
        }
        return result;
    }

    @CachedGauge(absolute = true, name = "streams.actions", timeout = 30, timeoutUnit = TimeUnit.SECONDS)
    public Map<String, String> getStreamActions() {
        Map<String, StreamStatusDTO> streams = streamStatusDao.getAll();
        Map<String, String> result = new HashMap<>();
        for (StreamStatusDTO stream : streams.values()) {
            result.put(stream.getStreamName(), stream.getActionsEnabled().toString());
        }
        return result;
    }
}
