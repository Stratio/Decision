package com.stratio.streaming.functions.dal;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;

import com.stratio.streaming.callbacks.StreamToBusCallback;
import com.stratio.streaming.commons.constants.REPLY_CODES;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.functions.StratioStreamingBaseFunction;

public class ListenStreamFunction extends StratioStreamingBaseFunction {
	
	private static Logger logger = LoggerFactory.getLogger(ListenStreamFunction.class);
	private ConcurrentHashMap<String, StreamToBusCallback> existingStreams = new ConcurrentHashMap<String, StreamToBusCallback>();

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
			
//			TODO if requested operation is DROP, then stop all existingStreams shutdown
			
//			stream exists
			if (getSiddhiManager().getInputHandler(request.getStreamName()) != null) {
				
				if (existingStreams.isEmpty() || !existingStreams.contains(request.getStreamName())) {
					
					
					try {
						StreamToBusCallback streamCallBack = new StreamToBusCallback(getSiddhiManager().getStreamDefinition(request.getStreamName()), getKafkaCluster());
						
						getSiddhiManager().addCallback(request.getStreamName(), streamCallBack);
						
						existingStreams.put(request.getStreamName(), streamCallBack);
						
						
						logger.info("==> LISTEN: stream " + request.getStreamName() + " is now working OK");
						
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
				logger.info("==> LISTEN: stream " + request.getStreamName() + " does not exist, no go to the LISTEN KO");
				ackStreamingOperation(request, REPLY_CODES.KO_STREAM_DOES_NOT_EXIST);
			}
		}
		
		
		
		return null;
	}

}
