package com.stratio.streaming.functions.dml;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;

import com.stratio.streaming.commons.messages.ListStreamsMessage;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.functions.StratioStreamingBaseFunction;
import com.stratio.streaming.streams.StreamOperations;
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
			
			List<StratioStreamingMessage> existingStreams = StreamOperations.listStreams(request, getSiddhiManager());
			
			ZKUtils.getZKUtils(zookeeperCluster).createZNodeJsonReply(request, new ListStreamsMessage(existingStreams.size(), //value.count
																										System.currentTimeMillis(), 					//value.time
																										existingStreams));                         		//value.streams																				

		}		
		
		return null;
	}

}
