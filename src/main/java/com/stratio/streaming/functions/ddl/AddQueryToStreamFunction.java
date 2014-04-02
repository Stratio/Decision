package com.stratio.streaming.functions.ddl;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.exception.DifferentDefinitionAlreadyExistException;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.exception.MalformedAttributeException;
import org.wso2.siddhi.query.compiler.exception.SiddhiPraserException;

import com.stratio.streaming.commons.constants.REPLY_CODES;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.functions.StratioStreamingBaseFunction;
import com.stratio.streaming.utils.SiddhiUtils;

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
				if (SiddhiUtils.getStreamStatus(request.getStreamName(), getSiddhiManager()).getAddedQueries().isEmpty() ||
						!SiddhiUtils.getStreamStatus(request.getStreamName(), getSiddhiManager()).getAddedQueries().containsValue(request.getRequest())) {
					
					try {

//						add query to siddhi
						String queryId = getSiddhiManager().addQuery(request.getRequest());
						
//						register query in stream status						
						SiddhiUtils.addQueryToStreamStatus(queryId, request.getRequest(), request.getStreamName(), getSiddhiManager());
						
//						check the streams to see if there are new ones, inferred from queries (not user defined)
						for(StreamDefinition streamMetaData : getSiddhiManager().getStreamDefinitions()) {
//							by getting the stream, it will be created if don't exists (user defined is false)
							SiddhiUtils.getStreamStatus(streamMetaData.getStreamId(), getSiddhiManager());
						}
						

//						ack OK to the Bus
						ackStreamingOperation(request, REPLY_CODES.OK);
						
					} 
					catch (MalformedAttributeException  se ) {
						ackStreamingOperation(request, REPLY_CODES.KO_COLUMN_DOES_NOT_EXIST);
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
