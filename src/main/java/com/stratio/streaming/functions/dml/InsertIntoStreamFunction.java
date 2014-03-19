package com.stratio.streaming.functions.dml;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.exception.AttributeNotExistException;

import com.stratio.streaming.common.StratioStreamingConstants;
import com.stratio.streaming.functions.StratioStreamingBaseFunction;
import com.stratio.streaming.messages.BaseStreamingMessage;
import com.stratio.streaming.messages.ColumnNameTypeValue;

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
	public Void call(JavaRDD<BaseStreamingMessage> rdd) throws Exception {
		
		List<BaseStreamingMessage> requests = rdd.collect();
		
		for (BaseStreamingMessage request : requests) {
			
//			stream exists
			if (getSiddhiManager().getInputHandler(request.getStreamName()) != null) {
				
				try {
					getSiddhiManager().getInputHandler(request.getStreamName()).send(getOrderedValues(request.getStreamName(), 
																											request.getColumns()));
					
					logger.info("==> INSERT: stream " + request.getStreamName() + " has received a new event OK");
					
					ackStreamingOperation(request, StratioStreamingConstants.REPLY_CODES.OK);
					
				} catch (AttributeNotExistException anee) {
					ackStreamingOperation(request, StratioStreamingConstants.REPLY_CODES.KO_COLUMN_DOES_NOT_EXIST);
				}
				catch (InterruptedException e) {
					ackStreamingOperation(request, StratioStreamingConstants.REPLY_CODES.KO_GENERAL_ERROR);
				}
				
				
			}
			else {
				logger.info("==> INSERT: stream " + request.getStreamName() + " does not exist, no go to the insert KO");
				ackStreamingOperation(request, StratioStreamingConstants.REPLY_CODES.KO_STREAM_DOES_NOT_EXIST);
			}
		}
		
		
		
		return null;
	}

	
	private Object[] getOrderedValues(String streamName, List<ColumnNameTypeValue> columns) {
		
		StreamDefinition streamMetaData = getSiddhiManager().getStreamDefinition(streamName);
		Object[] orderedValues = new Object[streamMetaData.getAttributeList().size()];
		
		for (ColumnNameTypeValue column : columns) {
			
			orderedValues[streamMetaData.getAttributePosition(column.getColumn())] = column.getValue();
		}

		return orderedValues;
		
	}
	



}
