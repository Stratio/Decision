/*******************************************************************************
 * Copyright 2014 Stratio
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.stratio.streaming.functions.dal;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;

import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.entity.Cells;
import com.stratio.streaming.callbacks.StreamToCassandraCallback;
import com.stratio.streaming.commons.constants.REPLY_CODES;
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.functions.StratioStreamingBaseFunction;
import com.stratio.streaming.streams.StreamOperations;
import com.stratio.streaming.streams.StreamSharedStatus;
import com.stratio.streaming.utils.SiddhiUtils;

public class SaveToCassandraStreamFunction extends StratioStreamingBaseFunction {
	
	private static Logger logger = LoggerFactory.getLogger(SaveToCassandraStreamFunction.class);
	private String cassandraCluster;
	private ConcurrentHashMap<String, IDeepJobConfig<Cells>> runningCassandraConfigs = new ConcurrentHashMap<String, IDeepJobConfig<Cells>>();
	

	/**
	 * 
	 */
	private static final long serialVersionUID = 7911766880059394316L;

	public SaveToCassandraStreamFunction(SiddhiManager siddhiManager, String zookeeperCluster, String kafkaCluster, String cassandraCluster) {
		super(siddhiManager, zookeeperCluster, kafkaCluster);
		this.cassandraCluster = cassandraCluster;		
	}
	

	@Override
	public Void call(JavaRDD<StratioStreamingMessage> rdd) throws Exception {
		
		List<StratioStreamingMessage> requests = rdd.collect();
		
		for (StratioStreamingMessage request : requests) {
			
			
//			stream is allowed and exists in siddhi
//			TODO add ack for not allowed operation 
			if (SiddhiUtils.isStreamAllowedForThisOperation(request.getStreamName(), STREAM_OPERATIONS.ACTION.SAVETO_CASSANDRA)
					&& getSiddhiManager().getStreamDefinition(request.getStreamName()) != null) {
		
//				check if save2cassandra is already enabled
				if (StreamSharedStatus.getStreamStatus(request.getStreamName(), getSiddhiManager()) != null
						&& !StreamSharedStatus.getStreamStatus(request.getStreamName(), getSiddhiManager()).isSaveToCassandra_enabled()) {
				
					try {
						
						StreamOperations.save2cassandraStream(request, cassandraCluster, getSiddhiManager());
						
						ackStreamingOperation(request, REPLY_CODES.OK);
						
						
						
					} catch (Exception e) {
						logger.info("<<<<<<<<<<<<<<<<<" + e.getMessage() + "//" + e.getClass() + "//" + e.getCause());
						ackStreamingOperation(request, REPLY_CODES.KO_GENERAL_ERROR);
					}
				}
				else {
					ackStreamingOperation(request, REPLY_CODES.KO_SAVE2CASSANDRA_STREAM_ALREADY_ENABLED);
				}
				
				
			}
			else {
				ackStreamingOperation(request, REPLY_CODES.KO_STREAM_DOES_NOT_EXIST);
			}
		}
		
		
		
		return null;
	}

}
