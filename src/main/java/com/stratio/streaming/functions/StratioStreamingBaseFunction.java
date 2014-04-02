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
