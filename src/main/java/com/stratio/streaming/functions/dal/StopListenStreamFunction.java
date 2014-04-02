package com.stratio.streaming.functions.dal;

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

public class StopListenStreamFunction extends StratioStreamingBaseFunction {
	
	private static Logger logger = LoggerFactory.getLogger(StopListenStreamFunction.class);
	private ITopic<String> listenTopic;

	/**
	 * 
	 */
	private static final long serialVersionUID = 7911766880059394316L;

	public StopListenStreamFunction(SiddhiManager siddhiManager, String zookeeperCluster, String kafkaCluster) {
		super(siddhiManager, zookeeperCluster, kafkaCluster);
		this.listenTopic = getSiddhiManager().getSiddhiContext().getHazelcastInstance().getTopic(STREAMING.INTERNAL_LISTEN_TOPIC);
	}
	

	@Override
	public Void call(JavaRDD<StratioStreamingMessage> rdd) throws Exception {
		
		List<StratioStreamingMessage> requests = rdd.collect();
		
		for (StratioStreamingMessage request : requests) {

			
//			stream exists
			if (getSiddhiManager().getStreamDefinition(request.getStreamName()) != null) {
				
				if (SiddhiUtils.getStreamStatus(request.getStreamName(), getSiddhiManager()).isListen_enabled()) {
					
					
					try {
																							
						listenTopic.publish(request.getStreamName());
						
						SiddhiUtils.changeListenerStreamStatus(Boolean.FALSE, request.getStreamName(), getSiddhiManager());																		
						
						ackStreamingOperation(request, REPLY_CODES.OK);
						
						
					} catch (Exception e) {
						ackStreamingOperation(request, REPLY_CODES.KO_GENERAL_ERROR);
					}
					
					
				}
				else {
					ackStreamingOperation(request, REPLY_CODES.KO_LISTENER_DOES_NOT_EXIST);
				}	
			}
			else {
				ackStreamingOperation(request, REPLY_CODES.KO_STREAM_DOES_NOT_EXIST);
			}
		}
		
		
		
		return null;
	}

}
