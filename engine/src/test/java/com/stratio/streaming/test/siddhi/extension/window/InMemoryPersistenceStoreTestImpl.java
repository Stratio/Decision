package com.stratio.streaming.test.siddhi.extension.window;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.event.management.PersistenceManagementEvent;
import org.wso2.siddhi.core.persistence.InMemoryPersistenceStore;
import org.wso2.siddhi.core.persistence.PersistenceObject;
import org.wso2.siddhi.core.persistence.PersistenceStore;

public class InMemoryPersistenceStoreTestImpl implements PersistenceStore {

    private static final Logger log = Logger.getLogger(InMemoryPersistenceStore.class);
    private final Map<String, Map<String, Map<String, PersistenceObject>>> persistenceMap = new HashMap<>();
    private final Map<String, List<String>> revisionMap = new HashMap<>();

    @Override
    public void save(PersistenceManagementEvent persistenceManagementEvent, String elementId, PersistenceObject data) {
        Map<String, Map<String, PersistenceObject>> executionPersistenceMap = persistenceMap
                .get(persistenceManagementEvent.getExecutionPlanIdentifier());
        if (executionPersistenceMap == null) {
            executionPersistenceMap = new HashMap<String, Map<String, PersistenceObject>>();
        }
        Map<String, PersistenceObject> executionRevisionMap = executionPersistenceMap.get(persistenceManagementEvent
                .getRevision());

        if (executionRevisionMap == null) {
            executionRevisionMap = new HashMap<String, PersistenceObject>();
            executionPersistenceMap.put(persistenceManagementEvent.getRevision(), executionRevisionMap);
        }
        data.setElementId(elementId);
        executionRevisionMap.put(elementId, data);
        if (log.isDebugEnabled()) {
            log.debug(elementId + " serialized");
        }

        List<String> revisionList = revisionMap.get(persistenceManagementEvent.getExecutionPlanIdentifier());
        if (revisionList == null) {
            revisionList = new ArrayList<String>();
            revisionMap.put(persistenceManagementEvent.getExecutionPlanIdentifier(), revisionList);
        }
        if (revisionList.size() == 0
                || (revisionList.size() > 0 && !persistenceManagementEvent.getRevision().equals(
                        revisionList.get(revisionList.size() - 1)))) {
            revisionList.add(persistenceManagementEvent.getRevision());
            revisionMap.put(persistenceManagementEvent.getExecutionPlanIdentifier(), revisionList);
        }
        persistenceMap.put(persistenceManagementEvent.getExecutionPlanIdentifier(), executionPersistenceMap);

    }

    @Override
    public PersistenceObject load(PersistenceManagementEvent persistenceManagementEvent, String elementId) {

        Map<String, Map<String, PersistenceObject>> executionPersistenceMap = persistenceMap
                .get(persistenceManagementEvent.getExecutionPlanIdentifier());
        if (executionPersistenceMap == null) {
            log.warn("Data not found for the execution plan " + persistenceManagementEvent.getExecutionPlanIdentifier());
            return null;
        }
        Map<String, PersistenceObject> executionRevisionMap = executionPersistenceMap.get(persistenceManagementEvent
                .getRevision());

        if (executionRevisionMap == null) {
            log.warn("Data not found for the revision  " + persistenceManagementEvent.getRevision()
                    + " of the execution plan " + persistenceManagementEvent.getExecutionPlanIdentifier());
            return null;
        }
        if (log.isDebugEnabled()) {
            log.debug("Deserializing " + elementId);
        }
        return executionRevisionMap.get(elementId);
    }

    @Override
    public String getLastRevision(String executionPlanIdentifier) {
        List<String> revisionList = revisionMap.get(executionPlanIdentifier);
        if (revisionList == null) {
            return null;
        }
        if (revisionList.size() > 0) {
            return revisionList.get(revisionList.size() - 1);
        }
        return null;
    }

    public PersistenceObject getPersistenceMap(String persistKey) {
        String key = persistKey.split("_")[1];
        return persistenceMap.get(key).get(persistKey).get(key.concat("-1"));
    }

}
