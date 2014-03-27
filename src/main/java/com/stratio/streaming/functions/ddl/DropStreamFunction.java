package com.stratio.streaming.functions.ddl;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;

import com.stratio.streaming.commons.constants.REPLY_CODES;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.functions.StratioStreamingBaseFunction;

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
//			so there is no need to check if stream exists before dropping it.
			if (getSiddhiManager().getInputHandler(request.getStreamName()) != null) {
				
				
//				we first remove all queries added to this stream
//				StreamStatus streamStatus = SiddhiUtils.getStreamStatus(request.getStreamName(), getSiddhiManager());
//				
//				for (String queryId: streamStatus.getAddedQueries().keySet()) {
//					
//					getSiddhiManager().removeQuery(queryId);
//				}
//				
////				then we removeStream in siddhi
				getSiddhiManager().removeStream(request.getStreamName());
//				
////				and finally we drop the streamStatus
//				SiddhiUtils.removeStreamStatus(request.getStreamName(), getSiddhiManager());
				
//				TODO notify all listeners to stop by using hazelcast topics
				
//				ack OK back to the bus
				ackStreamingOperation(request, REPLY_CODES.OK);
				
				logger.info("==> DROP: stream " + request.getStreamName() + " was removed OK");
			}
			else {				
				ackStreamingOperation(request, REPLY_CODES.KO_STREAM_DOES_NOT_EXIST);
				
				logger.info("==> DROP: stream " + request.getStreamName() + " does not exist KO");
			}
			
		}
		
		return null;
	}
}
