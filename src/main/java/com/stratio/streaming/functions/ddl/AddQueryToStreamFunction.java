package com.stratio.streaming.functions.ddl;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.exception.DifferentDefinitionAlreadyExistException;
import org.wso2.siddhi.query.api.exception.MalformedAttributeException;
import org.wso2.siddhi.query.api.exception.SourceNotExistException;
import org.wso2.siddhi.query.compiler.exception.SiddhiPraserException;

import com.stratio.streaming.commons.constants.REPLY_CODES;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.functions.StratioStreamingBaseFunction;
import com.stratio.streaming.streams.StreamOperations;
import com.stratio.streaming.streams.StreamSharedStatus;

public class AddQueryToStreamFunction extends StratioStreamingBaseFunction {
	
	private static Logger logger = LoggerFactory.getLogger(AddQueryToStreamFunction.class);	

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
			if (getSiddhiManager().getStreamDefinition(request.getStreamName()) != null) {

							
//				Siddhi will add a query even when the same query is already present
//				So it's better to maintain a reminder of which queries have been added
//				and to prevent adding queries twice.
				if (StreamSharedStatus.getStreamStatus(request.getStreamName(), getSiddhiManager()) != null && 
						(StreamSharedStatus.getStreamStatus(request.getStreamName(), getSiddhiManager()).getAddedQueries().isEmpty() ||
						!StreamSharedStatus.getStreamStatus(request.getStreamName(), getSiddhiManager()).getAddedQueries().containsValue(request.getRequest()))) {
					
					try {

						StreamOperations.addQueryToExistingStream(request, getSiddhiManager());
						

//						ack OK to the Bus
						ackStreamingOperation(request, REPLY_CODES.OK);
						
					} 
					catch (MalformedAttributeException  se ) {
						ackStreamingOperation(request, REPLY_CODES.KO_COLUMN_DOES_NOT_EXIST);
					}
					catch (SourceNotExistException snee) {
						ackStreamingOperation(request, REPLY_CODES.KO_SOURCE_STREAM_DOES_NOT_EXIST);
					}
					catch (SiddhiPraserException se) {
						ackStreamingOperation(request, REPLY_CODES.KO_PARSER_ERROR);
					}
					catch (DifferentDefinitionAlreadyExistException ddaee) {
						ackStreamingOperation(request, REPLY_CODES.KO_OUTPUTSTREAM_EXISTS_AND_DEFINITION_IS_DIFFERENT);
					}
					
				}			 		
				else {
					ackStreamingOperation(request, REPLY_CODES.KO_QUERY_ALREADY_EXISTS);
				}

			}
			else {
				ackStreamingOperation(request, REPLY_CODES.KO_STREAM_DOES_NOT_EXIST);
			}
			
		}

		return null;
	}


}
