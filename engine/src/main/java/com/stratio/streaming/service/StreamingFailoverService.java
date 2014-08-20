package com.stratio.streaming.service;

import com.stratio.streaming.dao.StreamStatusDao;
import com.stratio.streaming.dao.StreamingFailoverDao;
import com.stratio.streaming.model.CassandraPersistenceStoreModel;

public class StreamingFailoverService {

    private final StreamStatusDao streamStatusDao;
    private final StreamMetadataService streamMetadataService;
    private final StreamingFailoverDao streamingFailoverDao;

    public StreamingFailoverService(StreamStatusDao streamStatusDao, StreamMetadataService streamMetadataService,
            StreamingFailoverDao streamingFailoverDao) {
        this.streamStatusDao = streamStatusDao;
        this.streamMetadataService = streamMetadataService;
        this.streamingFailoverDao = streamingFailoverDao;
    }

    public synchronized void load() {
        CassandraPersistenceStoreModel cassandraPersistenceStoreModel = streamingFailoverDao.load();
        if (cassandraPersistenceStoreModel != null) {
            streamStatusDao.putAll(cassandraPersistenceStoreModel.getStreamStatuses());
            streamMetadataService.setSnapshot(cassandraPersistenceStoreModel.getSiddhiSnapshot());
        }
    }

    public synchronized void save() {
        streamingFailoverDao.save(new CassandraPersistenceStoreModel(streamStatusDao.getAll(), streamMetadataService
                .getSnapshot()));
    }

}
