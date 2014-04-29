package com.stratio.streaming.functions.requests;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;

import com.google.common.collect.Lists;
import com.stratio.streaming.commons.constants.STREAMING;
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.commons.messages.StreamQuery;
import com.stratio.streaming.functions.StratioStreamingBaseFunction;
import com.stratio.streaming.streams.StreamOperations;
import com.stratio.streaming.utils.SiddhiUtils;

public class CollectRequestForStatsFunction extends StratioStreamingBaseFunction {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 7911766880059394316L;
	private static Logger logger = LoggerFactory.getLogger(CollectRequestForStatsFunction.class);
	
	

	protected interface STATS_STREAMS {
		static final String BASE = "define stream " + STREAMING.STATS_NAMES.BASE + " (operation string, streamName string, count int)";
	}
	
	private interface STATS_QUERIES {
		
		static final String REQUEST_THROUGHPUT = "from " + STREAMING.STATS_NAMES.BASE + " #window.time( 1 sec ) " +
												 " select 'TROUGHPUT' as operation, '" + STREAMING.STATS_NAMES.GLOBAL_STATS_BY_OPERATION + "' as streamName, count(*) as data" +
												 " insert into " + STREAMING.STATS_NAMES.GLOBAL_STATS_BY_OPERATION + " for current-events";
		
		
		static final String GLOBAL_STATS_BY_OPERATION = "from " + STREAMING.STATS_NAMES.BASE +
														" select operation, '" + STREAMING.STATS_NAMES.GLOBAL_STATS_BY_OPERATION + "' as streamName, sum(count) as data group by operation insert into " + 
														STREAMING.STATS_NAMES.GLOBAL_STATS_BY_OPERATION + 
														" for current-events";
	}
	
	
	



	public CollectRequestForStatsFunction(SiddhiManager siddhiManager, String zookeeperCluster, String kafkaCluster) {
		super(siddhiManager, zookeeperCluster, kafkaCluster);
	}
	

	@Override
	public Void call(JavaRDD<StratioStreamingMessage> rdd) throws Exception {
				
		List<StratioStreamingMessage> requests = rdd.collect();
		
		for (StratioStreamingMessage request : requests) {
			
			List<ColumnNameTypeValue> selectedFields = Lists.newArrayList();
			
			selectedFields.add(new ColumnNameTypeValue("operation", null, request.getOperation().toUpperCase()));
			selectedFields.add(new ColumnNameTypeValue("streamName", null, request.getStreamName()));
			selectedFields.add(new ColumnNameTypeValue("count", null, Integer.valueOf(1)));
//			selectedFields.add(new ColumnNameTypeValue("index", null, getIndexForOperation(request.getOperation())));
			
			
			
			
			getStatsBaseStream().send(SiddhiUtils.getOrderedValues(getSiddhiManager().getStreamDefinition(STREAMING.STATS_NAMES.BASE), selectedFields));
		}

		return null;
	}
	
	
	private InputHandler getStatsBaseStream() throws Exception {
		
		if (getSiddhiManager().getStreamDefinition(STREAMING.STATS_NAMES.BASE) == null) {
			
			getSiddhiManager().defineStream(STATS_STREAMS.BASE);
			getSiddhiManager().addQuery(STATS_QUERIES.GLOBAL_STATS_BY_OPERATION);
			getSiddhiManager().addQuery(STATS_QUERIES.REQUEST_THROUGHPUT);
			
			sendResetValuesForAllOperations(getSiddhiManager().getInputHandler(STREAMING.STATS_NAMES.BASE));
			
//			StratioStreamingMessage message = new StratioStreamingMessage();
//			message.setStreamName(STATS_QUERIES.GLOBAL_STATS_BY_OPERATION);
//			StreamOperations.listenStream(message, getKafkaCluster(), getSiddhiManager());
			
		}
		
		return getSiddhiManager().getInputHandler(STREAMING.STATS_NAMES.BASE);
		
	}
	
	
	private void sendResetValuesForAllOperations(InputHandler baseRequestsStream) throws Exception {

		baseRequestsStream.send(SiddhiUtils.getOrderedValues(getSiddhiManager().getStreamDefinition(STREAMING.STATS_NAMES.BASE), 
								resetValuesForOperation(STREAM_OPERATIONS.ACTION.LISTEN)));
		
		baseRequestsStream.send(SiddhiUtils.getOrderedValues(getSiddhiManager().getStreamDefinition(STREAMING.STATS_NAMES.BASE), 
								resetValuesForOperation(STREAM_OPERATIONS.ACTION.SAVETO_CASSANDRA)));
				
		baseRequestsStream.send(SiddhiUtils.getOrderedValues(getSiddhiManager().getStreamDefinition(STREAMING.STATS_NAMES.BASE), 
								resetValuesForOperation(STREAM_OPERATIONS.DEFINITION.ADD_QUERY)));
		
		baseRequestsStream.send(SiddhiUtils.getOrderedValues(getSiddhiManager().getStreamDefinition(STREAMING.STATS_NAMES.BASE), 
								resetValuesForOperation(STREAM_OPERATIONS.DEFINITION.ALTER)));
		
		baseRequestsStream.send(SiddhiUtils.getOrderedValues(getSiddhiManager().getStreamDefinition(STREAMING.STATS_NAMES.BASE), 
								resetValuesForOperation(STREAM_OPERATIONS.DEFINITION.CREATE)));
		
		baseRequestsStream.send(SiddhiUtils.getOrderedValues(getSiddhiManager().getStreamDefinition(STREAMING.STATS_NAMES.BASE), 
								resetValuesForOperation(STREAM_OPERATIONS.DEFINITION.DROP)));
		
		baseRequestsStream.send(SiddhiUtils.getOrderedValues(getSiddhiManager().getStreamDefinition(STREAMING.STATS_NAMES.BASE), 
								resetValuesForOperation(STREAM_OPERATIONS.MANIPULATION.INSERT)));
		
		baseRequestsStream.send(SiddhiUtils.getOrderedValues(getSiddhiManager().getStreamDefinition(STREAMING.STATS_NAMES.BASE), 
								resetValuesForOperation(STREAM_OPERATIONS.MANIPULATION.LIST)));
		
		
	}
	
	
	private List<ColumnNameTypeValue> resetValuesForOperation(String operation) {
		List<ColumnNameTypeValue> selectedFields = Lists.newArrayList();
		
		selectedFields.add(new ColumnNameTypeValue("operation", null, operation));
		selectedFields.add(new ColumnNameTypeValue("streamName", null, "resetStats"));
		selectedFields.add(new ColumnNameTypeValue("count", null, Integer.valueOf(0)));
//		selectedFields.add(new ColumnNameTypeValue("index", null, getIndexForOperation(operation)));
		
		return selectedFields;
	}
	
	private Integer getIndexForOperation(String operation) {
		
		
		switch (operation.toUpperCase()) {
		case STREAM_OPERATIONS.ACTION.LISTEN:			
			return Integer.valueOf(53828);
		case STREAM_OPERATIONS.ACTION.SAVETO_CASSANDRA:			
			return Integer.valueOf(53829);		
		case STREAM_OPERATIONS.DEFINITION.ADD_QUERY:			
			return Integer.valueOf(53831);
		case STREAM_OPERATIONS.DEFINITION.ALTER:			
			return Integer.valueOf(53832);
		case STREAM_OPERATIONS.DEFINITION.CREATE:			
			return Integer.valueOf(53833);
		case STREAM_OPERATIONS.DEFINITION.DROP:			
			return Integer.valueOf(53834);
		case STREAM_OPERATIONS.MANIPULATION.INSERT:			
			return Integer.valueOf(53835);
		case STREAM_OPERATIONS.MANIPULATION.LIST:			
			return Integer.valueOf(53836);
		default:
			return Integer.valueOf(0);
		}
		
		
	}
	
	
	
}
