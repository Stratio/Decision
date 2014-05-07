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
/**
 * 
 */
package com.stratio.streaming.functions;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;

import com.stratio.streaming.commons.constants.REPLY_CODES;
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.utils.ZKUtils;

/**
 * @author dmorales
 *
 */
public abstract class StratioStreamingBaseFunction extends Function<JavaRDD<StratioStreamingMessage>, Void> {
	
	private static Logger logger = LoggerFactory.getLogger(StratioStreamingBaseFunction.class);
	
	private String zookeeperCluster;
	private String kafkaCluster;
	private transient SiddhiManager siddhiManager;

	/**
	 * 
	 */
	public StratioStreamingBaseFunction(SiddhiManager siddhiManager, String zookeeperCluster, String kafkaCluster) {
		super();
		this.zookeeperCluster = zookeeperCluster;
		this.kafkaCluster = kafkaCluster;
		this.siddhiManager = siddhiManager;
	}
	
	
	protected void ackStreamingOperation(StratioStreamingMessage request, Integer reply) throws Exception {		
		
		ZKUtils.getZKUtils(zookeeperCluster).createZNodeACK(request, reply);
		
		if (!request.getOperation().equalsIgnoreCase(STREAM_OPERATIONS.MANIPULATION.INSERT)) {
			
			logger.info("==> REQUEST HAS BEEN PROCESSED:" 
								+ " // stream:" 	+ request.getStreamName()  
								+ " // operation:"  + request.getOperation() 
								+ " // request:" 	+ request.getRequest()
								+ " // ACK:" 		+ REPLY_CODES.getReadableErrorFromCode(reply));
		}
	}


	protected SiddhiManager getSiddhiManager() {
		return siddhiManager;
	}


	protected String getZookeeperCluster() {
		return zookeeperCluster;
	}


	protected String getKafkaCluster() {
		return kafkaCluster;
	}
	
	
	

	

}
