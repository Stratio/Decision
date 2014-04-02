package com.stratio.streaming.functions.dml;

import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import com.google.common.collect.Lists;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.commons.messages.ListStreamsMessage;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.commons.messages.StreamQuery;
import com.stratio.streaming.functions.StratioStreamingBaseFunction;
import com.stratio.streaming.utils.SiddhiUtils;
import com.stratio.streaming.utils.ZKUtils;

public class ListStreamsFunction extends StratioStreamingBaseFunction {
	
	private static Logger logger = LoggerFactory.getLogger(ListStreamsFunction.class);
	private String zookeeperCluster;

	/**
	 * 
	 */
	private static final long serialVersionUID = 7911766880059394316L;

	public ListStreamsFunction(SiddhiManager siddhiManager, String zookeeperCluster, String kafkaCluster) {
		super(siddhiManager, zookeeperCluster, kafkaCluster);
		this.zookeeperCluster = zookeeperCluster;
	}
	

	@Override
	public Void call(JavaRDD<StratioStreamingMessage> rdd) throws Exception {
		
		List<StratioStreamingMessage> requests = rdd.collect();
		
		for (StratioStreamingMessage request : requests) {
			
			List<StratioStreamingMessage> streams = Lists.newArrayList();
			
			for(StreamDefinition streamMetaData : getSiddhiManager().getStreamDefinitions()) {
				
				List<ColumnNameTypeValue> columns = Lists.newArrayList();
				
				for (Attribute column : streamMetaData.getAttributeList()) {
					columns.add(new ColumnNameTypeValue(column.getName(), column.getType().toString(), null));
				}
				
				List<StreamQuery> queries = Lists.newArrayList();
				
				HashMap<String, String> attachedQueries = SiddhiUtils.getStreamStatus(streamMetaData.getStreamId(), getSiddhiManager()).getAddedQueries();
				
				for (Entry<String, String> entry : attachedQueries.entrySet()) {
					queries.add(new StreamQuery(entry.getKey(), entry.getValue()));				    
				}
				
				StratioStreamingMessage streamMessage = new StratioStreamingMessage(streamMetaData.getId(), columns, queries);
				streamMessage.isUserDefined(SiddhiUtils.getStreamStatus(streamMetaData.getStreamId(), getSiddhiManager()).isUserDefined());
				
				streams.add(streamMessage);
			}
			
			ZKUtils.getZKUtils(zookeeperCluster).createZNodeJsonReply(request, new ListStreamsMessage(getSiddhiManager().getStreamDefinitions().size(), //value.count
																										System.currentTimeMillis(), 					//value.time
																										streams));                                		//value.streams																				
			
			
		}
		
		
		
		return null;
	}

}
