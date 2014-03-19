/**
 * 
 */
package com.stratio.streaming.functions;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.wso2.siddhi.core.SiddhiManager;

import com.stratio.streaming.messages.BaseStreamingMessage;
import com.stratio.streaming.utils.ZKUtils;

/**
 * @author dmorales
 *
 */
public abstract class StratioStreamingBaseFunction extends Function<JavaRDD<BaseStreamingMessage>, Void> {
	
	private String zookeeperCluster;
	private String kafkaCluster;
	private SiddhiManager siddhiManager;

	/**
	 * 
	 */
	public StratioStreamingBaseFunction(SiddhiManager siddhiManager, String zookeeperCluster, String kafkaCluster) {
		super();
		this.zookeeperCluster = zookeeperCluster;
		this.kafkaCluster = kafkaCluster;
		this.siddhiManager = siddhiManager;
	}
	
	
	protected void ackStreamingOperation(BaseStreamingMessage request, Integer reply) throws Exception {
		
		ZKUtils.getZKUtils(zookeeperCluster).createZNode(request, reply);
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
