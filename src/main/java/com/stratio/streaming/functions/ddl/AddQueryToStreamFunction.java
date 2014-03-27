package com.stratio.streaming.functions.ddl;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.query.api.exception.MalformedAttributeException;
import org.wso2.siddhi.query.compiler.exception.SiddhiPraserException;

import com.stratio.streaming.commons.constants.REPLY_CODES;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.functions.StratioStreamingBaseFunction;

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
	public Void call(JavaRDD<StratioStreamingMessage> rdd) throws Exception {

		List<StratioStreamingMessage> requests = rdd.collect();
		
		for (StratioStreamingMessage request : requests) {

//			stream exists in siddhi
			if (getSiddhiManager().getInputHandler(request.getStreamName()) != null) {		
				
				if (addedQueries.isEmpty() || !addedQueries.contains(request.getRequest())) {

				
				
//				logger.info(">>>>>>>>>>>>>>>>>>>>>>>>> number of queries: " + SiddhiUtils.getStreamStatus(request.getStreamName(), getSiddhiManager()).getAddedQueries().size());
//				logger.info(">>>>>>>>>>>>>>>>>>>>>>>>> query is present:" + SiddhiUtils.getStreamStatus(request.getStreamName(), getSiddhiManager()).getAddedQueries().containsValue(request.getRequest()));
//				
////				Siddhi will add a query even when the same query is already present
////				So it's better to maintain a reminder of which queries have been added
////				and to prevent adding queries twice.
//				if (SiddhiUtils.getStreamStatus(request.getStreamName(), getSiddhiManager()).getAddedQueries().isEmpty() ||
//						!SiddhiUtils.getStreamStatus(request.getStreamName(), getSiddhiManager()).getAddedQueries().containsValue(request.getRequest())) {
//					
					try {

//						add query to siddhi
						String queryId = getSiddhiManager().addQuery(request.getRequest());
						
//						register query in stream status
//						StreamStatus streamStatus = SiddhiUtils.getStreamStatus(request.getStreamName(), getSiddhiManager());
//						streamStatus.getAddedQueries().put(queryId, request.getRequest());
//						SiddhiUtils.updateStreamStatus(streamStatus, getSiddhiManager());
						
						addedQueries.add(request.getRequest());
						
						
//						ack OK to the Bus
						ackStreamingOperation(request, REPLY_CODES.OK);
						
					} catch (SiddhiPraserException | MalformedAttributeException  se ) {
						logger.info("==> ADD QUERY: query " + request.getRequest() + " is not valid " + se.getMessage() + " ADD KO");
						ackStreamingOperation(request, REPLY_CODES.KO_PARSER_ERROR);
					}
				}					
				else {
					logger.info("==> ADD QUERY: query " + request.getRequest() + " already exists ADD KO");
					ackStreamingOperation(request, REPLY_CODES.KO_QUERY_ALREADY_EXISTS);
				}

			}
			else {
				logger.info("==> ADD QUERY: stream " + request.getStreamName() + " does not exist, no go to the SELECT KO");
				ackStreamingOperation(request, REPLY_CODES.KO_STREAM_DOES_NOT_EXIST);
			}
			
		}

		return null;
	}
}
