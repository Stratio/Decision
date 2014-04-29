package com.stratio.streaming.utils;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiConfiguration;
import org.wso2.siddhi.query.api.QueryFactory;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.Attribute.Type;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.exception.AttributeNotExistException;
import org.wso2.siddhi.query.compiler.exception.SiddhiPraserException;

import com.stratio.streaming.commons.constants.STREAMING;
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.streams.Casandra2PersistenceStore;
import com.stratio.streaming.streams.StreamPersistence;

public class SiddhiUtils {
	
	private static Logger logger = LoggerFactory.getLogger(SiddhiUtils.class);
	
	
	public static final String SIDDHI_TYPE_STRING 		= "STRING";
	public static final String SIDDHI_TYPE_BOOLEAN 		= "BOOLEAN";
	public static final String SIDDHI_TYPE_DOUBLE 		= "DOUBLE";
	public static final String SIDDHI_TYPE_INT 			= "INTEGER";
	public static final String SIDDHI_TYPE_LONG 		= "LONG";
	public static final String SIDDHI_TYPE_FLOAT 		= "FLOAT";
	
	public static final String QUERY_PLAN_IDENTIFIER   = "StratioStreamingCEP-Cluster";
	
	
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
	
	
	public static String recoverStreamDefinition(StreamDefinition streamDefinition) {
		
		String attributesList = "";
		
		for (Attribute field: streamDefinition.getAttributeList()) {
			attributesList += field.getName() + " " + field.getType().toString().toLowerCase() + ",";
		}
		
		
		return "define stream " + streamDefinition.getStreamId() + "(" + attributesList.substring(0, attributesList.length() -1) + ")";
	}
	
	
	public static StreamDefinition buildDefineStreamSiddhiQL(StratioStreamingMessage request) {
		
		StreamDefinition newStream = QueryFactory.createStreamDefinition().name(request.getStreamName());
		
		for (ColumnNameTypeValue column : request.getColumns()) {
			newStream.attribute(column.getColumn(), SiddhiUtils.decodeSiddhiType(column.getType()));			
		}			
		
		return newStream;
	}
	
	public static boolean columnAlreadyExistsInStream(String columnName, StreamDefinition streamMetaData) {
		
		for(Attribute column:  streamMetaData.getAttributeList()) {
			if (column.getName().equalsIgnoreCase(columnName)) {
				return true;
			}
		}
		
		return false;
	}
	
	
	/**
	 * 
	 * - Instantiates the Siddhi CEP engine
	 * - return the running CEP engine 
	 * 
	 * 
	 * @return SiddhiManager
	 */
	public static SiddhiManager setupSiddhiManager() {
		
		
		SiddhiConfiguration conf = new SiddhiConfiguration();	
		conf.setInstanceIdentifier("StratioStreamingCEP-Instance-"+ UUID.randomUUID().toString());
		conf.setQueryPlanIdentifier(QUERY_PLAN_IDENTIFIER);
		conf.setDistributedProcessing(true);			
		
		
		// Create Siddhi Manager
		SiddhiManager siddhiManager = new SiddhiManager(conf);
		
		
		siddhiManager.setPersistStore(new Casandra2PersistenceStore("node.stratio.com", "", ""));
				
		StreamPersistence.restoreLastRevision(siddhiManager);
		
		
		return siddhiManager;						
	}
	
	public static Object[] getOrderedValues(StreamDefinition streamMetaData, List<ColumnNameTypeValue> columns) throws AttributeNotExistException {
		
		Object[] orderedValues = new Object[streamMetaData.getAttributeList().size()];
		
		for (ColumnNameTypeValue column : columns) {

			
//			if attribute does not exist, a AttributeNotExistException exception will be thrown
			if (column.getValue() instanceof String) {
				orderedValues[streamMetaData.getAttributePosition(column.getColumn())] = decodeSiddhiValue((String) column.getValue(), streamMetaData.getAttributeType(column.getColumn()));
			}
			else {
				orderedValues[streamMetaData.getAttributePosition(column.getColumn())] = column.getValue();
			}
			
		}

		return orderedValues;
		
	}
	
	
	private static Object decodeSiddhiValue(String originalValue, Attribute.Type type) throws SiddhiPraserException {
		
		switch (type.toString()) {
			case SIDDHI_TYPE_STRING:
				return originalValue;
			case SIDDHI_TYPE_BOOLEAN:
				return Boolean.valueOf(originalValue);
			case SIDDHI_TYPE_DOUBLE:
				return Double.valueOf(originalValue);
			case SIDDHI_TYPE_INT:
				return Integer.valueOf(originalValue);
			case SIDDHI_TYPE_LONG:
				return Long.valueOf(originalValue);
			case SIDDHI_TYPE_FLOAT:
				return Float.valueOf(originalValue);
			default:
				throw new SiddhiPraserException("Unsupported Column type");
		}
		
	}
	
//	TODO move to StreamingCommons
	public static Boolean isStreamAllowedForThisOperation(String streamName, String operation) {
		
		switch (operation.toUpperCase()) {
			case STREAM_OPERATIONS.DEFINITION.ADD_QUERY:
			case STREAM_OPERATIONS.DEFINITION.ALTER:
			case STREAM_OPERATIONS.DEFINITION.CREATE:
			case STREAM_OPERATIONS.DEFINITION.DROP:
			case STREAM_OPERATIONS.DEFINITION.REMOVE_QUERY:
			case STREAM_OPERATIONS.MANIPULATION.INSERT:
				if (Arrays.asList(STREAMING.STATS_NAMES.STATS_STREAMS).contains(streamName)) {
					return Boolean.FALSE;
				}
				return Boolean.TRUE;
			
			case STREAM_OPERATIONS.ACTION.LISTEN:
			case STREAM_OPERATIONS.ACTION.SAVETO_CASSANDRA:
			case STREAM_OPERATIONS.ACTION.STOP_LISTEN:

				return Boolean.TRUE;
			default:
				return Boolean.FALSE;
		}						
	}
	
	

	

}
