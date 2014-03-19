package com.stratio.streaming.functions.dml;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;

import com.stratio.streaming.callbacks.SiddhiToKafkaStreamCallback;
import com.stratio.streaming.common.StratioStreamingConstants;
import com.stratio.streaming.functions.StratioStreamingBaseFunction;
import com.stratio.streaming.messages.BaseStreamingMessage;

public class ListenStreamFunction extends StratioStreamingBaseFunction {
	
	private static Logger logger = LoggerFactory.getLogger(ListenStreamFunction.class);
	private ConcurrentHashMap<String, SiddhiToKafkaStreamCallback> existingStreams = new ConcurrentHashMap<String, SiddhiToKafkaStreamCallback>();

	/**
	 * 
	 */
	private static final long serialVersionUID = 7911766880059394316L;

	public ListenStreamFunction(SiddhiManager siddhiManager, String zookeeperCluster, String kafkaCluster) {
		super(siddhiManager, zookeeperCluster, kafkaCluster);
	}
	

	@Override
	public Void call(JavaRDD<BaseStreamingMessage> rdd) throws Exception {
		
		List<BaseStreamingMessage> requests = rdd.collect();
		
		for (BaseStreamingMessage request : requests) {
			
//			TODO if requested operation is DROP, then stop all existingStreams shutdown
			
//			stream exists
			if (getSiddhiManager().getInputHandler(request.getStreamName()) != null) {
				
				if (existingStreams.isEmpty() || !existingStreams.contains(request.getStreamName())) {
					
					
					try {
						SiddhiToKafkaStreamCallback siddhiToKafkaStreamCallback = new SiddhiToKafkaStreamCallback(getSiddhiManager().getStreamDefinition(request.getStreamName()), getKafkaCluster());
						
						getSiddhiManager().addCallback(request.getStreamName(), siddhiToKafkaStreamCallback);
						
						existingStreams.put(request.getStreamName(), siddhiToKafkaStreamCallback);
						
						
						logger.info("==> LISTEN: stream " + request.getStreamName() + " is now working OK");
						
						ackStreamingOperation(request, StratioStreamingConstants.REPLY_CODES.OK);
					} catch (Exception e) {
						ackStreamingOperation(request, StratioStreamingConstants.REPLY_CODES.KO_GENERAL_ERROR);
					}
					
					
				}
				else {
					ackStreamingOperation(request, StratioStreamingConstants.REPLY_CODES.KO_LISTENER_ALREADY_EXISTS);
				}
				
					
			}
			else {
				logger.info("==> LISTEN: stream " + request.getStreamName() + " does not exist, no go to the LISTEN KO");
				ackStreamingOperation(request, StratioStreamingConstants.REPLY_CODES.KO_STREAM_DOES_NOT_EXIST);
			}
		}
		
		
		
		return null;
	}

}
