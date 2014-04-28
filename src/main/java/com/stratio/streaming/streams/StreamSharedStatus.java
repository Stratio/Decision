package com.stratio.streaming.streams;

import java.util.Arrays;

import org.wso2.siddhi.core.SiddhiManager;

import com.hazelcast.core.IMap;
import com.stratio.streaming.commons.constants.STREAMING;
import com.stratio.streaming.utils.SiddhiUtils;

public class StreamSharedStatus {

	private StreamSharedStatus() {

	}
	
	
	
	public static StreamStatusDTO createStreamStatus(String streamName, SiddhiManager siddhiManager) {
		
		IMap<Object, Object> streamStatusMap = siddhiManager.getSiddhiContext().getHazelcastInstance().getMap(STREAMING.STREAM_STATUS_MAP);
		StreamStatusDTO streamStatusDTO = new StreamStatusDTO(streamName, Boolean.TRUE);
		streamStatusDTO.setStreamDefinition(SiddhiUtils.recoverStreamDefinition(siddhiManager.getStreamDefinition(streamName)));
		streamStatusMap.put(streamName, streamStatusDTO);
		return streamStatusDTO;
		
	}
	
	
	public static void updateStreamDefinitionStreamStatus(String streamName, SiddhiManager siddhiManager) {
		
		IMap<Object, Object> streamStatusMap = siddhiManager.getSiddhiContext().getHazelcastInstance().getMap(STREAMING.STREAM_STATUS_MAP);
		StreamStatusDTO streamStatusDTO = (StreamStatusDTO) streamStatusMap.get(streamName);
		
		streamStatusDTO.setStreamDefinition(SiddhiUtils.recoverStreamDefinition(siddhiManager.getStreamDefinition(streamName)));
		streamStatusMap.put(streamName, streamStatusDTO);
	}



	public static StreamStatusDTO getStreamStatus(String streamName, SiddhiManager siddhiManager) {	
		
		IMap<Object, Object> streamStatusMap = siddhiManager.getSiddhiContext().getHazelcastInstance().getMap(STREAMING.STREAM_STATUS_MAP);
		
		if (streamStatusMap.get(streamName) != null) {
			return (StreamStatusDTO) streamStatusMap.get(streamName);
		}
		else {
//			stream status does not exist, this is an special case
//			the stream exists in siddhi becase a previous query has created it
//			so we are going to register it as new
			
//			avoid returning stats streams
			if (Arrays.asList(STREAMING.STATS_NAMES.RESERVED_STREAMS).contains(streamName)) {
				return null;
			}
			
			StreamStatusDTO streamStatusDTO = new StreamStatusDTO(streamName, Boolean.FALSE);
			streamStatusDTO.setStreamDefinition(SiddhiUtils.recoverStreamDefinition(siddhiManager.getStreamDefinition(streamName)));
			streamStatusMap.put(streamName, streamStatusDTO);
			return streamStatusDTO;
		}
		
	}
	
	public static void addQueryToStreamStatus(String queryId, String query, String streamName, SiddhiManager siddhiManager) {
		
		IMap<Object, Object> streamStatusMap = siddhiManager.getSiddhiContext().getHazelcastInstance().getMap(STREAMING.STREAM_STATUS_MAP);
		StreamStatusDTO streamStatusDTO = (StreamStatusDTO) streamStatusMap.get(streamName);
		streamStatusDTO.getAddedQueries().put(queryId, query);				
		streamStatusMap.put(streamStatusDTO.getStreamName(), streamStatusDTO);
		
		
	}
	
	public static String removeQueryInStreamStatus(String queryId, String streamName, SiddhiManager siddhiManager) {
		
		IMap<Object, Object> streamStatusMap = siddhiManager.getSiddhiContext().getHazelcastInstance().getMap(STREAMING.STREAM_STATUS_MAP);
		StreamStatusDTO streamStatusDTO = (StreamStatusDTO) streamStatusMap.get(streamName);		
		streamStatusDTO.getAddedQueries().remove(queryId);		
		streamStatusMap.put(streamStatusDTO.getStreamName(), streamStatusDTO);		
		return queryId;
	}
	
	public static void changeListenerStreamStatus(Boolean enabled, String streamName, SiddhiManager siddhiManager) {
		
		IMap<Object, Object> streamStatusMap = siddhiManager.getSiddhiContext().getHazelcastInstance().getMap(STREAMING.STREAM_STATUS_MAP);
		StreamStatusDTO streamStatusDTO = (StreamStatusDTO) streamStatusMap.get(streamName);
		streamStatusDTO.setListen_enabled(enabled);
		streamStatusMap.put(streamStatusDTO.getStreamName(), streamStatusDTO);
		
		
	}
	
	public static void removeStreamStatus(String streamName, SiddhiManager siddhiManager) {
		
		IMap<Object, Object> streamStatusMap = siddhiManager.getSiddhiContext().getHazelcastInstance().getMap(STREAMING.STREAM_STATUS_MAP);
		
		streamStatusMap.remove(streamName);
		
	}
	
	

	
	public static void changeSave2CassandraStreamStatus(Boolean enabled, String streamName, SiddhiManager siddhiManager) {
		
		IMap<Object, Object> streamStatusMap = siddhiManager.getSiddhiContext().getHazelcastInstance().getMap(STREAMING.STREAM_STATUS_MAP);
		StreamStatusDTO streamStatusDTO = (StreamStatusDTO) streamStatusMap.get(streamName);
		streamStatusDTO.setSaveToCassandra_enabled(enabled);
		streamStatusMap.put(streamStatusDTO.getStreamName(), streamStatusDTO);
		
		
	}
	

}
