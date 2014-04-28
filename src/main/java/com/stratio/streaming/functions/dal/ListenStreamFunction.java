package com.stratio.streaming.functions.dal;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;

import com.stratio.streaming.commons.constants.REPLY_CODES;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.functions.StratioStreamingBaseFunction;
import com.stratio.streaming.streams.StreamOperations;
import com.stratio.streaming.streams.StreamSharedStatus;

public class ListenStreamFunction extends StratioStreamingBaseFunction {
	
	private static Logger logger = LoggerFactory.getLogger(ListenStreamFunction.class);

	/**
	 * 
	 */
	private static final long serialVersionUID = 7911766880059394316L;

	public ListenStreamFunction(SiddhiManager siddhiManager, String zookeeperCluster, String kafkaCluster) {
		super(siddhiManager, zookeeperCluster, kafkaCluster);
	}
	

	@Override
	public Void call(JavaRDD<StratioStreamingMessage> rdd) throws Exception {
		
		List<StratioStreamingMessage> requests = rdd.collect();
		
		for (StratioStreamingMessage request : requests) {
			
		
//			stream exists
			if (getSiddhiManager().getStreamDefinition(request.getStreamName()) != null) {
				
				if (StreamSharedStatus.getStreamStatus(request.getStreamName(), getSiddhiManager()) != null
						&& !StreamSharedStatus.getStreamStatus(request.getStreamName(), getSiddhiManager()).isListen_enabled()) {
					
					
					try {
						
						StreamOperations.listenStream(request, getKafkaCluster(), getSiddhiManager());

						
						ackStreamingOperation(request, REPLY_CODES.OK);
						
						
					} catch (Exception e) {
						ackStreamingOperation(request, REPLY_CODES.KO_GENERAL_ERROR);
					}
					
					
				}
				else {
					ackStreamingOperation(request, REPLY_CODES.KO_LISTENER_ALREADY_EXISTS);
				}
				
					
			}
			else {
				ackStreamingOperation(request, REPLY_CODES.KO_STREAM_DOES_NOT_EXIST);
			}
		}
		
		
		
		return null;
	}

}
