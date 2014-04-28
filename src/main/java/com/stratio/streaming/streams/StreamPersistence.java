package com.stratio.streaming.streams;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;

import com.hazelcast.core.IMap;
import com.stratio.streaming.commons.constants.STREAMING;
import com.stratio.streaming.utils.SiddhiUtils;

public class StreamPersistence {
	
	private static Logger logger = LoggerFactory.getLogger(StreamPersistence.class);

	private StreamPersistence() {
		// TODO Auto-generated constructor stub
	}

	
	

	
	
	public static String saveStreamingEngineStatus(SiddhiManager siddhiManager) {
		
		String revisionID = siddhiManager.persist();		
		String executionPlan = "";
		
		IMap<Object, Object> streamStatusMap = siddhiManager.getSiddhiContext().getHazelcastInstance().getMap(STREAMING.STREAM_STATUS_MAP);
		
		for (Object stream : streamStatusMap.values()) {						
			
			executionPlan += ((StreamStatusDTO) stream).getStreamDefinition() + ";";
			
			for (String query : ((StreamStatusDTO) stream).getAddedQueries().values()) {
				executionPlan += query + ";";
			}
			
		}
		
		
		logger.debug("StreamPersistence: revisionID: " + revisionID + "//executionPlan: " + executionPlan);
		
		if (!executionPlan.equalsIgnoreCase("")) {
		
			Casandra2PersistenceStore cassandraPersistence = (Casandra2PersistenceStore) siddhiManager.getSiddhiContext().getPersistenceService().getPersistenceStore();
			
			cassandraPersistence.saveExecutionPlan(executionPlan);
		}
		
		
		return revisionID;
		
	}
	
	
	public static void removeEngineStatusFromCleanExit(SiddhiManager siddhiManager) {
		
		Casandra2PersistenceStore cassandraPersistence = (Casandra2PersistenceStore) siddhiManager.getSiddhiContext().getPersistenceService().getPersistenceStore();
		
		cassandraPersistence.removeAllRevisions();
		
	}
	
	
	public static void restoreLastRevision(SiddhiManager siddhiManager) {
				
		Casandra2PersistenceStore cassandraPersistence = (Casandra2PersistenceStore) siddhiManager.getSiddhiContext().getPersistenceService().getPersistenceStore();

		String recoveredExecutionPlan = cassandraPersistence.recoverLastExecutionPlan(SiddhiUtils.QUERY_PLAN_IDENTIFIER);
		
		if (recoveredExecutionPlan != null && !recoveredExecutionPlan.equals("")) {
			logger.debug("StreamPersistence: recovered execution plan from last execution -> " + recoveredExecutionPlan);
			siddhiManager.addExecutionPlan(recoveredExecutionPlan);
		}
		
		siddhiManager.restoreLastRevision();

	}
}
