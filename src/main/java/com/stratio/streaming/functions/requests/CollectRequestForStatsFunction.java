package com.stratio.streaming.functions.requests;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;

import com.google.common.collect.Lists;
import com.stratio.streaming.common.StratioStreamingConstants;
import com.stratio.streaming.functions.StratioStreamingBaseFunction;
import com.stratio.streaming.messages.BaseStreamingMessage;
import com.stratio.streaming.messages.ColumnNameTypeValue;
import com.stratio.streaming.utils.SiddhiUtils;

public class CollectRequestForStatsFunction extends StratioStreamingBaseFunction {
	
	private static Logger logger = LoggerFactory.getLogger(CollectRequestForStatsFunction.class);
	
	
	protected interface STATS_NAMES {
		static final String BASE = "stratio_stats_base";
		static final String GLOBAL_STATS_BY_OPERATION = "stratio_stats_global_by_operation";
		
	}
	protected interface STATS_STREAMS {
		static final String BASE = "define stream " + STATS_NAMES.BASE + " (operation string, streamName string, index int, count int)";
	}
	
	private interface STATS_QUERIES {
		static final String GLOBAL_STATS_BY_OPERATION = "from " + STATS_NAMES.BASE + 
														" select operation, streamName, index, sum(count) as data group by operation output snapshot every 5 sec insert into " + 
														STATS_NAMES.GLOBAL_STATS_BY_OPERATION + 
														" for current-events";
	}
	
	
	

	/**
	 * 
	 */
	private static final long serialVersionUID = 7911766880059394316L;

	public CollectRequestForStatsFunction(SiddhiManager siddhiManager, String zookeeperCluster, String kafkaCluster) {
		super(siddhiManager, zookeeperCluster, kafkaCluster);
	}
	

	@Override
	public Void call(JavaRDD<BaseStreamingMessage> rdd) throws Exception {
		
				
		List<BaseStreamingMessage> requests = rdd.collect();
		
		for (BaseStreamingMessage request : requests) {
			
			List<ColumnNameTypeValue> selectedFields = Lists.newArrayList();
			
			selectedFields.add(new ColumnNameTypeValue("operation", null, request.getOperation()));
			selectedFields.add(new ColumnNameTypeValue("streamName", null, request.getStreamName()));
			selectedFields.add(new ColumnNameTypeValue("count", null, Integer.valueOf(1)));
			selectedFields.add(new ColumnNameTypeValue("index", null, getIndexForOperation(request.getOperation())));
			
			
			
			
			getStatsBaseStream().send(SiddhiUtils.getOrderedValues(getSiddhiManager().getStreamDefinition(STATS_NAMES.BASE), selectedFields));
		}
		
		

		return null;
	}
	
	
	private InputHandler getStatsBaseStream() {
		
		if (getSiddhiManager().getInputHandler(STATS_NAMES.BASE) == null) {
			
			getSiddhiManager().defineStream(STATS_STREAMS.BASE);
			getSiddhiManager().addQuery(STATS_QUERIES.GLOBAL_STATS_BY_OPERATION);
			
		}
		
		return getSiddhiManager().getInputHandler(STATS_NAMES.BASE);
		
	}
	
	private Integer getIndexForOperation(String operation) {
		
		
		switch (operation.toUpperCase()) {
		case StratioStreamingConstants.STREAM_OPERATIONS.ACTION.LISTEN:			
			return Integer.valueOf(53828);
		case StratioStreamingConstants.STREAM_OPERATIONS.ACTION.SAVETO_CASSANDRA:			
			return Integer.valueOf(53829);
		case StratioStreamingConstants.STREAM_OPERATIONS.ACTION.SAVETO_DATACOLLECTOR:			
			return Integer.valueOf(53830);
		case StratioStreamingConstants.STREAM_OPERATIONS.DEFINITION.ADD_QUERY:			
			return Integer.valueOf(53831);
		case StratioStreamingConstants.STREAM_OPERATIONS.DEFINITION.ALTER:			
			return Integer.valueOf(53832);
		case StratioStreamingConstants.STREAM_OPERATIONS.DEFINITION.CREATE:			
			return Integer.valueOf(53833);
		case StratioStreamingConstants.STREAM_OPERATIONS.DEFINITION.DROP:			
			return Integer.valueOf(53834);
		case StratioStreamingConstants.STREAM_OPERATIONS.MANIPULATION.INSERT:			
			return Integer.valueOf(53835);
		case StratioStreamingConstants.STREAM_OPERATIONS.MANIPULATION.LIST:			
			return Integer.valueOf(53836);

		default:
			return Integer.valueOf(0);
		}
		
		
	}
	
	
	
}
