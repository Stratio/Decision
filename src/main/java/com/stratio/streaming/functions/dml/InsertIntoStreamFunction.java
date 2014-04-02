package com.stratio.streaming.functions.dml;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.query.api.exception.AttributeNotExistException;

import com.stratio.streaming.commons.constants.REPLY_CODES;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.functions.StratioStreamingBaseFunction;
import com.stratio.streaming.utils.SiddhiUtils;

public class InsertIntoStreamFunction extends StratioStreamingBaseFunction {
	
	private static Logger logger = LoggerFactory.getLogger(InsertIntoStreamFunction.class);

	/**
	 * 
	 */
	private static final long serialVersionUID = 7911766880059394316L;

	public InsertIntoStreamFunction(SiddhiManager siddhiManager, String zookeeperCluster, String kafkaCluster) {
		super(siddhiManager, zookeeperCluster, kafkaCluster);
	}
	

	@Override
	public Void call(JavaRDD<StratioStreamingMessage> rdd) throws Exception {
		
		List<StratioStreamingMessage> requests = rdd.collect();
		
		for (StratioStreamingMessage request : requests) {
			
//			stream exists
			if (getSiddhiManager().getStreamDefinition(request.getStreamName()) != null) {
				
				try {
					getSiddhiManager()
						.getInputHandler(request.getStreamName())
							.send(SiddhiUtils.getOrderedValues(getSiddhiManager().getStreamDefinition(request.getStreamName()), 
																											request.getColumns()));
					
					
//					ackStreamingOperation(request, StratioStreamingConstants.REPLY_CODES.OK);
					
				} catch (AttributeNotExistException anee) {
					ackStreamingOperation(request, REPLY_CODES.KO_COLUMN_DOES_NOT_EXIST);
				}
				catch (InterruptedException e) {
					ackStreamingOperation(request, REPLY_CODES.KO_GENERAL_ERROR);
				}
				
				
			}
			else {
				ackStreamingOperation(request, REPLY_CODES.KO_STREAM_DOES_NOT_EXIST);
			}
		}
		
		
		
		return null;
	}

	

	



}
