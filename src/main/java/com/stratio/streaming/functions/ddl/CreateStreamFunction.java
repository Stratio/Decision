package com.stratio.streaming.functions.ddl;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.query.compiler.exception.SiddhiPraserException;

import com.stratio.streaming.common.StratioStreamingConstants;
import com.stratio.streaming.functions.StratioStreamingBaseFunction;
import com.stratio.streaming.messages.BaseStreamingMessage;
import com.stratio.streaming.messages.ColumnNameTypeValue;

public class CreateStreamFunction extends StratioStreamingBaseFunction {
	
	private static Logger logger = LoggerFactory.getLogger(CreateStreamFunction.class);

	/**
	 * 
	 */
	private static final long serialVersionUID = 7911766880059394316L;

	public CreateStreamFunction(SiddhiManager siddhiManager, String zookeeperCluster, String kafkaCluster) {
		super(siddhiManager, zookeeperCluster, kafkaCluster);
	}
	
	private String buildDefineStreamSiddhiQL(BaseStreamingMessage request) {
		
		String siddhiQL = "define stream " + request.getStreamName();
		String columns = " ";
		
		for (ColumnNameTypeValue column : request.getColumns()) {
			columns += column.getColumn() + " " + column.getType() + ",";
		}			
		
		return siddhiQL + "(" + columns.substring(0, columns.length() -1) + ")";
	}
	

	@Override
	public Void call(JavaRDD<BaseStreamingMessage> rdd) throws Exception {
		
		List<BaseStreamingMessage> requests = rdd.collect();
		
		for (BaseStreamingMessage request : requests) {
//			Siddhi doesn't throw an exception if stream exists and you try to recreate it in the same way
//			but if you try to redefine a stream, siddhi will throw a DifferentDefinitionAlreadyExistException
//			so, we avoid this scenario but checking first if stream exists
			if (getSiddhiManager().getInputHandler(request.getStreamName()) == null) {
				
				
				try {
					
					logger.info("==========================> " + buildDefineStreamSiddhiQL(request));
					getSiddhiManager().defineStream(buildDefineStreamSiddhiQL(request));
					logger.info("==> CREATE: new stream " + request.getStreamName() + " OK");
					ackStreamingOperation(request, StratioStreamingConstants.REPLY_CODES.OK);
				} catch (SiddhiPraserException spe) {
					ackStreamingOperation(request, StratioStreamingConstants.REPLY_CODES.KO_PARSER_ERROR);
				}
				
			}
			else {
				logger.info("==> CREATE: stream " + request.getStreamName() + " already exists KO");
				ackStreamingOperation(request, StratioStreamingConstants.REPLY_CODES.KO_STREAM_ALREADY_EXISTS);
			}
		}
		
		return null;
	}
}
