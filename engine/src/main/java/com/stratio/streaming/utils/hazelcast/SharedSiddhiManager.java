package com.stratio.streaming.utils.hazelcast;

import com.stratio.streaming.siddhi.extension.DistinctWindowExtension;
import com.stratio.streaming.utils.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiConfiguration;
import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.persistence.PersistenceStore;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.tracer.EventMonitor;
import org.wso2.siddhi.core.util.ExecutionPlanReference;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.definition.TableDefinition;
import org.wso2.siddhi.query.api.definition.partition.PartitionDefinition;
import org.wso2.siddhi.query.api.query.Query;
import org.wso2.siddhi.query.compiler.exception.SiddhiParserException;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


public class SharedSiddhiManager extends SiddhiManager {

    private static final Logger log = LoggerFactory.getLogger(SharedSiddhiManager.class);

    public static final String QUERY_PLAN_IDENTIFIER = "StratioStreamingCEP-Cluster";

    private SiddhiManager siddhiManager;

    private Cache<String, SiddhiManager> siddhiCache;

    public SharedSiddhiManager(StreamingHazelcastInstance streamingHazelcastInstance) {
        initCache(streamingHazelcastInstance);
        this.siddhiManager = getFromCacheOrCreate();
    }

    private void initCache(StreamingHazelcastInstance streamingHazelcastInstance) {
        siddhiCache = new HazelcastCache<>(streamingHazelcastInstance.getHazelcastInstance(), "siddhi");
    }

    private SiddhiManager getFromCacheOrCreate() {
        log.info("Trying to retrieve SiddhiManager from cache");
        SiddhiManager cachedManager = siddhiCache.get("manager");
        if(cachedManager == null) {
            log.info("Not cached SiddhiManager found, creating a new one and caching it.");
            SiddhiConfiguration conf = new SiddhiConfiguration();
            conf.setInstanceIdentifier("StratioStreamingCEP-Instance-" + UUID.randomUUID().toString());
            conf.setQueryPlanIdentifier(QUERY_PLAN_IDENTIFIER);
            conf.setDistributedProcessing(false);

            @SuppressWarnings("rawtypes")
            List<Class> extensions = new ArrayList<>();
            extensions.add(DistinctWindowExtension.class);
            conf.setSiddhiExtensions(extensions);

            // Create Siddhi Manager
            cachedManager = new SiddhiManager(conf);
            siddhiCache.put("manager", cachedManager);
        } else {
            log.info("SiddhiManager found in cache.");
        }
        return cachedManager;
    }

    @Override
    public InputHandler defineStream(StreamDefinition streamDefinition) {
        return siddhiManager.defineStream(streamDefinition);
    }

    @Override
    public InputHandler defineStream(String streamDefinition) throws SiddhiParserException {
        return siddhiManager.defineStream(streamDefinition);
    }

    @Override
    public void removeStream(String streamId) {
        siddhiManager.removeStream(streamId);
    }

    @Override
    public void defineTable(TableDefinition tableDefinition) {
        siddhiManager.defineTable(tableDefinition);
    }

    @Override
    public void defineTable(String tableDefinition) throws SiddhiParserException {
        siddhiManager.defineTable(tableDefinition);
    }

    @Override
    public void removeTable(String tableId) {
        siddhiManager.removeTable(tableId);
    }

    @Override
    public void definePartition(PartitionDefinition partitionDefinition) {
        siddhiManager.definePartition(partitionDefinition);
    }

    @Override
    public void definePartition(String partitionDefinition) throws SiddhiParserException {
        siddhiManager.definePartition(partitionDefinition);
    }

    @Override
    public void removePartition(String partitionId) {
        siddhiManager.removePartition(partitionId);
    }

    @Override
    public String addQuery(String query) throws SiddhiParserException {
        return siddhiManager.addQuery(query);
    }

    @Override
    public String addQuery(Query query) {
        return siddhiManager.addQuery(query);
    }

    @Override
    public ExecutionPlanReference addExecutionPlan(String executionPlan) throws SiddhiParserException {
        return siddhiManager.addExecutionPlan(executionPlan);
    }

    @Override
    public void removeQuery(String queryId) {
        siddhiManager.removeQuery(queryId);
    }

    @Override
    public Query getQuery(String queryReference) {
        return siddhiManager.getQuery(queryReference);
    }

    @Override
    public InputHandler getInputHandler(String streamId) {
        return siddhiManager.getInputHandler(streamId);
    }

    @Override
    public void addCallback(String streamId, StreamCallback streamCallback) {
        siddhiManager.addCallback(streamId, streamCallback);
    }

    @Override
    public void addCallback(String queryReference, QueryCallback callback) {
        siddhiManager.addCallback(queryReference, callback);
    }

    @Override
    public void shutdown() {
        siddhiManager.shutdown();
    }

    @Override
    public StreamDefinition getStreamDefinition(String streamId) {
        return siddhiManager.getStreamDefinition(streamId);
    }

    @Override
    public List<StreamDefinition> getStreamDefinitions() {
        return siddhiManager.getStreamDefinitions();
    }

    @Override
    public void setPersistStore(PersistenceStore persistStore) {
        siddhiManager.setPersistStore(persistStore);
    }

    @Override
    public void setEventMonitor(EventMonitor eventMonitor) {
        siddhiManager.setEventMonitor(eventMonitor);
    }

    @Override
    public void enableStats(boolean enableStats) {
        siddhiManager.enableStats(enableStats);
    }

    @Override
    public void enableTrace(boolean enableTrace) {
        siddhiManager.enableTrace(enableTrace);
    }

    @Override
    public String persist() {
        return siddhiManager.persist();
    }

    @Override
    public void restoreRevision(String revision) {
        siddhiManager.restoreRevision(revision);
    }

    @Override
    public void restoreLastRevision() {
        siddhiManager.restoreLastRevision();
    }

    @Override
    public byte[] snapshot() {
        return siddhiManager.snapshot();
    }

    @Override
    public void restore(byte[] snapshot) {
        siddhiManager.restore(snapshot);
    }

    @Override
    public SiddhiContext getSiddhiContext() {
        return siddhiManager.getSiddhiContext();
    }
}
