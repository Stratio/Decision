package com.stratio.streaming.functions.ddl;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;

import com.stratio.streaming.commons.constants.REPLY_CODES;
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.functions.StratioStreamingBaseFunction;
import com.stratio.streaming.streams.StreamOperations;
import com.stratio.streaming.streams.StreamSharedStatus;
import com.stratio.streaming.utils.SiddhiUtils;

public class DropStreamFunction extends StratioStreamingBaseFunction {
	
	private static Logger logger = LoggerFactory.getLogger(DropStreamFunction.class);
	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 7911766880059394316L;

	public DropStreamFunction(SiddhiManager siddhiManager, String zookeeperCluster, String kafkaCluster) {
		super(siddhiManager, zookeeperCluster, kafkaCluster);
	}
	

	@Override
	public Void call(JavaRDD<StratioStreamingMessage> rdd) throws Exception {
		
		List<StratioStreamingMessage> requests = rdd.collect();
		
		for (StratioStreamingMessage request : requests) {
						
//			Siddhi doesn't throw an exception if stream does not exist and you try to remove it
			
//			stream is allowed and exists in siddhi
			if (SiddhiUtils.isStreamAllowedForThisOperation(request.getStreamName(), STREAM_OPERATIONS.DEFINITION.DROP)
					&& getSiddhiManager().getStreamDefinition(request.getStreamName()) != null) {

				
				if (StreamSharedStatus.getStreamStatus(request.getStreamName(), getSiddhiManager()) != null
						&& StreamSharedStatus.getStreamStatus(request.getStreamName(), getSiddhiManager()).isUserDefined()) {
				

					StreamOperations.dropStream(request, getSiddhiManager());
								
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
