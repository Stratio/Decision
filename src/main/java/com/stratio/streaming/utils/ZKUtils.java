package com.stratio.streaming.utils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.streaming.common.StratioStreamingConstants;
import com.stratio.streaming.messages.BaseStreamingMessage;

public class ZKUtils {
	
	private static Logger logger = LoggerFactory.getLogger(ZKUtils.class);
	
	private static ZKUtils self;
	private String zookeeperCluster;
	private CuratorFramework  client;

	private ZKUtils(String zookeeperCluster) throws Exception {
		this.zookeeperCluster = zookeeperCluster;
				
//		ZOOKEPER CONNECTION
		client = CuratorFrameworkFactory.newClient(zookeeperCluster, 25*1000, 10*1000, new ExponentialBackoffRetry(1000, 3));
		client.start();
		client.getZookeeperClient().blockUntilConnectedOrTimedOut();
		
		if (!client.isStarted()) {
			 throw new Exception("Connection to Zookeeper timed out after seconds");
		}
		
	}
	
	
	public static ZKUtils getZKUtils(String zookeeperCluster) throws Exception {
		if (self == null) {
			self = new ZKUtils(zookeeperCluster);			
		}
		return self;
	}
	
	
	public void createZNode(BaseStreamingMessage request, Integer reply) throws Exception {
		
		String path = StratioStreamingConstants.REPLY_CODES.ZK_BASE_PATH 
													+ "/" + request.getOperation().toLowerCase()
													+ "/" + request.getRequest_id();
		
		
		if (client.checkExists().forPath(path) != null) {
			client.delete().deletingChildrenIfNeeded().forPath(path);
		}
		
		client.create().creatingParentsIfNeeded().forPath(path, reply.toString().getBytes());
		
		logger.info("**** ZKUTILS " + request.getOperation() + "//" + request.getRequest_id() + "//" + reply + "//" + path);
		
	}
}
