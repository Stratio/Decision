package com.stratio.streaming.functions.ddl;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.query.compiler.exception.SiddhiPraserException;

import com.stratio.streaming.commons.constants.REPLY_CODES;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.functions.StratioStreamingBaseFunction;

public class CreateStreamFunction extends StratioStreamingBaseFunction {
	
	private static Logger logger = LoggerFactory.getLogger(CreateStreamFunction.class);

	/**
	 * 
	 */
	private static final long serialVersionUID = 7911766880059394316L;

	public CreateStreamFunction(SiddhiManager siddhiManager, String zookeeperCluster, String kafkaCluster) {
		super(siddhiManager, zookeeperCluster, kafkaCluster);
	}
	
	private String buildDefineStreamSiddhiQL(StratioStreamingMessage request) {
		
		String siddhiQL = "define stream " + request.getStreamName();
		String columns = " ";
		
		for (ColumnNameTypeValue column : request.getColumns()) {
			columns += column.getColumn() + " " + column.getType() + ",";
		}			
		
		return siddhiQL + "(" + columns.substring(0, columns.length() -1) + ")";
	}
	

	@Override
	public Void call(JavaRDD<StratioStreamingMessage> rdd) throws Exception {
		
		List<StratioStreamingMessage> requests = rdd.collect();
		
		for (StratioStreamingMessage request : requests) {
//			Siddhi doesn't throw an exception if stream exists and you try to recreate it in the same way
//			but if you try to redefine a stream, siddhi will throw a DifferentDefinitionAlreadyExistException
//			so, we avoid this scenario but checking first if stream exists
			if (getSiddhiManager().getInputHandler(request.getStreamName()) == null) {
				
				
				try {
										
//					create stream in siddhi
					getSiddhiManager().defineStream(buildDefineStreamSiddhiQL(request));
					
//					register stream in shared memory
//					SiddhiUtils.registerStreamStatus(request.getStreamName(), getSiddhiManager());
					
//					ack OK back to the bus
					ackStreamingOperation(request, REPLY_CODES.OK);
					
					logger.info("==> CREATE: new stream " + request.getStreamName() + " OK");
					
					
				} catch (SiddhiPraserException spe) {
					ackStreamingOperation(request, REPLY_CODES.KO_PARSER_ERROR);
				}
				
			}
			else {
				logger.info("==> CREATE: stream " + request.getStreamName() + " already exists KO");
				ackStreamingOperation(request, REPLY_CODES.KO_STREAM_ALREADY_EXISTS);
			}
		}
		
		return null;
	}
}
