package com.stratio.streaming.functions.ddl;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.query.api.exception.MalformedAttributeException;
import org.wso2.siddhi.query.compiler.exception.SiddhiPraserException;

import com.stratio.streaming.common.StratioStreamingConstants;
import com.stratio.streaming.functions.StratioStreamingBaseFunction;
import com.stratio.streaming.messages.BaseStreamingMessage;

public class AddQueryToStreamFunction extends StratioStreamingBaseFunction {
	
	private static Logger logger = LoggerFactory.getLogger(AddQueryToStreamFunction.class);	
	private CopyOnWriteArrayList<String> addedQueries = new CopyOnWriteArrayList<String>();

	/**
	 * 
	 */
	private static final long serialVersionUID = 7911766880059394316L;

	public AddQueryToStreamFunction(SiddhiManager siddhiManager, String zookeeperCluster, String kafkaCluster) {
		super(siddhiManager, zookeeperCluster, kafkaCluster);
	}
	

	@Override
	public Void call(JavaRDD<BaseStreamingMessage> rdd) throws Exception {

		List<BaseStreamingMessage> requests = rdd.collect();
		
		for (BaseStreamingMessage request : requests) {

//			stream exists
			if (getSiddhiManager().getInputHandler(request.getStreamName()) != null) {
				
//				Siddhi will add a query even when the same query is already present
//				So it's better to maintain a reminder of which queries have been added
//				and to prevent adding queries twice.
				if (addedQueries.isEmpty() || !addedQueries.contains(request.getRequest())) {
					try {
						getSiddhiManager().addQuery(request.getRequest());
						addedQueries.add(request.getRequest());
						ackStreamingOperation(request, StratioStreamingConstants.REPLY_CODES.OK);
					} catch (SiddhiPraserException | MalformedAttributeException  se ) {
						logger.info("==> ADD QUERY: query " + request.getRequest() + " is not valid " + se.getMessage() + " ADD KO");
						ackStreamingOperation(request, StratioStreamingConstants.REPLY_CODES.KO_PARSER_ERROR);
					}
				}					
				else {
					logger.info("==> ADD QUERY: query " + request.getRequest() + " already exists ADD KO");
					ackStreamingOperation(request, StratioStreamingConstants.REPLY_CODES.KO_QUERY_ALREADY_EXISTS);
				}

			}
			else {
				logger.info("==> ADD QUERY: stream " + request.getStreamName() + " does not exist, no go to the SELECT KO");
				ackStreamingOperation(request, StratioStreamingConstants.REPLY_CODES.KO_STREAM_DOES_NOT_EXIST);
			}
			
		}

		return null;
	}
}
