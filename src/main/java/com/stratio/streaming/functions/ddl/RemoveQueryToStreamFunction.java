package com.stratio.streaming.functions.ddl;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.query.api.exception.MalformedAttributeException;
import org.wso2.siddhi.query.compiler.exception.SiddhiPraserException;

import com.hazelcast.core.ITopic;
import com.stratio.streaming.commons.constants.REPLY_CODES;
import com.stratio.streaming.commons.constants.STREAMING;
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.functions.StratioStreamingBaseFunction;
import com.stratio.streaming.streams.StreamOperations;
import com.stratio.streaming.streams.StreamSharedStatus;
import com.stratio.streaming.utils.SiddhiUtils;

public class RemoveQueryToStreamFunction extends StratioStreamingBaseFunction {
	
	private static Logger logger = LoggerFactory.getLogger(RemoveQueryToStreamFunction.class);
	private ITopic<String> listenTopic;

	/**
	 * 
	 */
	private static final long serialVersionUID = 7911766880059394316L;

	public RemoveQueryToStreamFunction(SiddhiManager siddhiManager, String zookeeperCluster, String kafkaCluster) {
		super(siddhiManager, zookeeperCluster, kafkaCluster);
		this.listenTopic = getSiddhiManager().getSiddhiContext().getHazelcastInstance().getTopic(STREAMING.INTERNAL_LISTEN_TOPIC);
	}
	

	@Override
	public Void call(JavaRDD<StratioStreamingMessage> rdd) throws Exception {

		List<StratioStreamingMessage> requests = rdd.collect();
		
		for (StratioStreamingMessage request : requests) {

				
//			stream is allowed and exists in siddhi
			if (SiddhiUtils.isStreamAllowedForThisOperation(request.getStreamName(), STREAM_OPERATIONS.DEFINITION.REMOVE_QUERY)
					&& getSiddhiManager().getStreamDefinition(request.getStreamName()) != null) {


				
//				check if query has been added before
				if (StreamSharedStatus.getStreamStatus(request.getStreamName(), getSiddhiManager()) != null &&
						StreamSharedStatus.getStreamStatus(request.getStreamName(), getSiddhiManager()).getAddedQueries().containsKey(request.getRequest())) {
					
					try {

						StreamOperations.removeQueryFromExistingStream(request, getSiddhiManager());

//						ack OK to the Bus
						ackStreamingOperation(request, REPLY_CODES.OK);
						
					} 
					catch (SiddhiPraserException | MalformedAttributeException  se ) {
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
