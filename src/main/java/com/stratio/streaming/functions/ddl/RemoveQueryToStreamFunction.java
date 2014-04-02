package com.stratio.streaming.functions.ddl;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.query.api.exception.MalformedAttributeException;
import org.wso2.siddhi.query.compiler.exception.SiddhiPraserException;

import com.stratio.streaming.commons.constants.REPLY_CODES;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.functions.StratioStreamingBaseFunction;
import com.stratio.streaming.streams.StreamStatus;
import com.stratio.streaming.utils.SiddhiUtils;

public class RemoveQueryToStreamFunction extends StratioStreamingBaseFunction {
	
	private static Logger logger = LoggerFactory.getLogger(RemoveQueryToStreamFunction.class);	

	/**
	 * 
	 */
	private static final long serialVersionUID = 7911766880059394316L;

	public RemoveQueryToStreamFunction(SiddhiManager siddhiManager, String zookeeperCluster, String kafkaCluster) {
		super(siddhiManager, zookeeperCluster, kafkaCluster);
	}
	

	@Override
	public Void call(JavaRDD<StratioStreamingMessage> rdd) throws Exception {

		List<StratioStreamingMessage> requests = rdd.collect();
		
		for (StratioStreamingMessage request : requests) {

//			stream exists in siddhi
			if (getSiddhiManager().getStreamDefinition(request.getStreamName()) != null) {

				logger.info("-----------> stream:" + request.getStreamName() + "//request:" +  request.getRequest() + "//isPresent" + SiddhiUtils.getStreamStatus(request.getStreamName(), getSiddhiManager()).getAddedQueries().containsKey(request.getRequest()));			
				
//				check if query has been added before
				if (SiddhiUtils.getStreamStatus(request.getStreamName(), getSiddhiManager()).getAddedQueries().containsKey(request.getRequest())) {
					
					try {

						String queryId = SiddhiUtils.removeQueryInStreamStatus(request.getRequest(), request.getStreamName(), getSiddhiManager());
						
//						remove query in siddhi
						getSiddhiManager().removeQuery(queryId);

//						ack OK to the Bus
						ackStreamingOperation(request, REPLY_CODES.OK);
						
					} catch (SiddhiPraserException | MalformedAttributeException  se ) {
						ackStreamingOperation(request, REPLY_CODES.KO_PARSER_ERROR);
					}
				}					
				else {
					ackStreamingOperation(request, REPLY_CODES.KO_QUERY_DOES_NOT_EXIST);
				}

			}
			else {
				ackStreamingOperation(request, REPLY_CODES.KO_STREAM_DOES_NOT_EXIST);
			}
			
		}

		return null;
	}
}
