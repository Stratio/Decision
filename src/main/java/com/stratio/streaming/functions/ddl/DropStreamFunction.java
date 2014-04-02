package com.stratio.streaming.functions.ddl;

import java.util.HashMap;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;

import com.hazelcast.core.ITopic;
import com.stratio.streaming.commons.constants.REPLY_CODES;
import com.stratio.streaming.commons.constants.STREAMING;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.functions.StratioStreamingBaseFunction;
import com.stratio.streaming.utils.SiddhiUtils;

public class DropStreamFunction extends StratioStreamingBaseFunction {
	
	private static Logger logger = LoggerFactory.getLogger(DropStreamFunction.class);
	private ITopic<String> listenTopic;
	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 7911766880059394316L;

	public DropStreamFunction(SiddhiManager siddhiManager, String zookeeperCluster, String kafkaCluster) {
		super(siddhiManager, zookeeperCluster, kafkaCluster);
		this.listenTopic = getSiddhiManager().getSiddhiContext().getHazelcastInstance().getTopic(STREAMING.INTERNAL_LISTEN_TOPIC);
	}
	

	@Override
	public Void call(JavaRDD<StratioStreamingMessage> rdd) throws Exception {
		
		List<StratioStreamingMessage> requests = rdd.collect();
		
		for (StratioStreamingMessage request : requests) {
						
//			Siddhi doesn't throw an exception if stream does not exist and you try to remove it
//			so there is no need to check if stream exists before dropping it.
			if (getSiddhiManager().getStreamDefinition(request.getStreamName()) != null) {
				
				if (SiddhiUtils.getStreamStatus(request.getStreamName(), getSiddhiManager()).isUserDefined()) {
				
//					stop all listeners
					listenTopic.publish(request.getStreamName());		
					
//					remove all queries
					HashMap<String, String> attachedQueries = SiddhiUtils.getStreamStatus(request.getStreamName(), getSiddhiManager()).getAddedQueries();
					
					for (String queryId : attachedQueries.keySet()) {
						getSiddhiManager().removeQuery(queryId);
					}
					
//					then we removeStream in siddhi
					getSiddhiManager().removeStream(request.getStreamName());
	
//					drop the streamStatus
					SiddhiUtils.removeStreamStatus(request.getStreamName(), getSiddhiManager());
					
								
//					ack OK back to the bus
					ackStreamingOperation(request, REPLY_CODES.OK);
				}
				else {
					ackStreamingOperation(request, REPLY_CODES.KO_STREAM_IS_NOT_USER_DEFINED);
				}
			}
			else {				
				ackStreamingOperation(request, REPLY_CODES.KO_STREAM_DOES_NOT_EXIST);
			}
			
		}
		
		return null;
	}
}
