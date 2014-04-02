package com.stratio.streaming.utils;

import java.util.List;
import java.util.Map.Entry;

import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.Attribute.Type;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.exception.AttributeNotExistException;
import org.wso2.siddhi.query.compiler.exception.SiddhiPraserException;

import com.hazelcast.core.IMap;
import com.stratio.streaming.commons.constants.STREAMING;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.streams.StreamStatus;

public class SiddhiUtils {
	
	private static final String SIDDHI_TYPE_STRING 		= "STRING";
	private static final String SIDDHI_TYPE_BOOLEAN 	= "BOOLEAN";
	private static final String SIDDHI_TYPE_DOUBLE 		= "DOUBLE";
	private static final String SIDDHI_TYPE_INT 		= "INTEGER";
	private static final String SIDDHI_TYPE_LONG 		= "LONG";
	private static final String SIDDHI_TYPE_FLOAT 		= "FLOAT";
	
	
	private SiddhiUtils() {
		
	}
	

	
	public static Type decodeSiddhiType(String originalType) throws SiddhiPraserException {
		
		switch (originalType.toUpperCase()) {
			case SIDDHI_TYPE_STRING:
				return Attribute.Type.STRING;
			case SIDDHI_TYPE_BOOLEAN:
				return Attribute.Type.BOOL;
			case SIDDHI_TYPE_DOUBLE:
				return Attribute.Type.DOUBLE;
			case SIDDHI_TYPE_INT:
				return Attribute.Type.INT;
			case SIDDHI_TYPE_LONG:
				return Attribute.Type.LONG;
			case SIDDHI_TYPE_FLOAT:
				return Attribute.Type.FLOAT;
			default:
				throw new SiddhiPraserException("Unsupported Column type");
		}
		
	}
	
	public static Object[] getOrderedValues(StreamDefinition streamMetaData, List<ColumnNameTypeValue> columns) throws AttributeNotExistException {
		
		Object[] orderedValues = new Object[streamMetaData.getAttributeList().size()];
		
		for (ColumnNameTypeValue column : columns) {
			
//			if attribute does not exist, a AttributeNotExistException exception wil be thrown
			orderedValues[streamMetaData.getAttributePosition(column.getColumn())] = column.getValue();
		}

		return orderedValues;
		
	}

	
	public static StreamStatus createStreamStatus(String streamName, SiddhiManager siddhiManager) {
		
		IMap<Object, Object> streamStatusMap = siddhiManager.getSiddhiContext().getHazelcastInstance().getMap(STREAMING.STREAM_STATUS_MAP);
		StreamStatus streamStatus = new StreamStatus(streamName, Boolean.TRUE);
		streamStatusMap.put(streamName, streamStatus);
		return streamStatus;
		
	}



	public static StreamStatus getStreamStatus(String streamName, SiddhiManager siddhiManager) {
		
		IMap<Object, Object> streamStatusMap = siddhiManager.getSiddhiContext().getHazelcastInstance().getMap(STREAMING.STREAM_STATUS_MAP);
		
		if (streamStatusMap.get(streamName) != null) {
			return (StreamStatus) streamStatusMap.get(streamName);
		}
		else {
//			stream status does not exist, this is an special case
//			the stream exists in siddhi becase a previous query has created it
//			so we are going to register it as new
			StreamStatus streamStatus = new StreamStatus(streamName, Boolean.FALSE);
			streamStatusMap.put(streamName, streamStatus);
			return streamStatus;
		}
		
	}
	
	public static void addQueryToStreamStatus(String queryId, String query, String streamName, SiddhiManager siddhiManager) {
		
		IMap<Object, Object> streamStatusMap = siddhiManager.getSiddhiContext().getHazelcastInstance().getMap(STREAMING.STREAM_STATUS_MAP);
		StreamStatus streamStatus = (StreamStatus) streamStatusMap.get(streamName);
		streamStatus.getAddedQueries().put(queryId, query);				
		streamStatusMap.put(streamStatus.getStreamName(), streamStatus);
		
		
	}
	
	public static String removeQueryInStreamStatus(String queryId, String streamName, SiddhiManager siddhiManager) {
		
		IMap<Object, Object> streamStatusMap = siddhiManager.getSiddhiContext().getHazelcastInstance().getMap(STREAMING.STREAM_STATUS_MAP);
		StreamStatus streamStatus = (StreamStatus) streamStatusMap.get(streamName);		
		streamStatus.getAddedQueries().remove(queryId);		
		streamStatusMap.put(streamStatus.getStreamName(), streamStatus);		
		return queryId;
	}
	
	public static void changeListenerStreamStatus(Boolean enabled, String streamName, SiddhiManager siddhiManager) {
		
		IMap<Object, Object> streamStatusMap = siddhiManager.getSiddhiContext().getHazelcastInstance().getMap(STREAMING.STREAM_STATUS_MAP);
		StreamStatus streamStatus = (StreamStatus) streamStatusMap.get(streamName);
		streamStatus.setListen_enabled(enabled);
		streamStatusMap.put(streamStatus.getStreamName(), streamStatus);
		
		
	}
	
	
	
	public static void removeStreamStatus(String streamName, SiddhiManager siddhiManager) {
		
		IMap<Object, Object> streamStatusMap = siddhiManager.getSiddhiContext().getHazelcastInstance().getMap(STREAMING.STREAM_STATUS_MAP);
		
		streamStatusMap.remove(streamName);
		
	}
	

}
