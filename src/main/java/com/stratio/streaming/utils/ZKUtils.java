package com.stratio.streaming.utils;

import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.stratio.streaming.commons.constants.STREAMING;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;

public class ZKUtils {
	
	private static Logger logger = LoggerFactory.getLogger(ZKUtils.class);
	
	private static ZKUtils self;
	private CuratorFramework  client;
	private ExecutorService backgroundZookeeperCleanerTasks;

	private ZKUtils(String zookeeperCluster) throws Exception {
				
//		ZOOKEPER CONNECTION
		client = CuratorFrameworkFactory.newClient(zookeeperCluster, 25*1000, 10*1000, new ExponentialBackoffRetry(1000, 3));
		client.start();
		client.getZookeeperClient().blockUntilConnectedOrTimedOut();
		
		if (client.getState().compareTo(CuratorFrameworkState.STARTED) != 0) {
			 throw new Exception("Connection to Zookeeper timed out after seconds");
		}
		
		
		backgroundZookeeperCleanerTasks = Executors.newFixedThreadPool(1);
		backgroundZookeeperCleanerTasks.submit(new ZookeeperBackgroundCleaner(client));
		
	}
	
	
	public static ZKUtils getZKUtils(String zookeeperCluster) throws Exception {
		if (self == null) {
			self = new ZKUtils(zookeeperCluster);			
		}
		return self;
	}
	
	public static void shutdownZKUtils() {
		if (self != null) {
			self.backgroundZookeeperCleanerTasks.shutdownNow();
			self.client.close();
		}
	}
	
	public void createEphemeralZNode(String path, byte[] data) throws Exception {
		
		if (client.checkExists().forPath(path) != null) {
			client.delete().deletingChildrenIfNeeded().forPath(path);
		}
		
		client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path, data);
	}
	
	
	public void createZNodeACK(StratioStreamingMessage request, Integer reply) throws Exception {
		
		String path = STREAMING.ZK_BASE_PATH 
							+ "/" + request.getOperation().toLowerCase()
							+ "/" + request.getRequest_id();
		
		
		if (client.checkExists().forPath(path) != null) {
			client.delete().deletingChildrenIfNeeded().forPath(path);
		}
		
		client.create().creatingParentsIfNeeded().forPath(path, reply.toString().getBytes());
		
		logger.info("**** ZKUTILS " + request.getOperation() + "//" + request.getRequest_id() + "//" + reply + "//" + path);
		
	}
	
	
	public void createZNodeJsonReply(StratioStreamingMessage request, Object reply) throws Exception {
		
		String path = STREAMING.ZK_BASE_PATH 
							+ "/" + request.getOperation().toLowerCase()
							+ "/" + request.getRequest_id();
		
		
		if (client.checkExists().forPath(path) != null) {
			client.delete().deletingChildrenIfNeeded().forPath(path);
		}
		
		client.create().creatingParentsIfNeeded().forPath(path, new Gson().toJson(reply).getBytes());
		
		logger.info("**** ZKUTILS " + request.getOperation() + "//" + request.getRequest_id() + "//" + reply + "//" + path);
		
	}
	
	
	
	
	private class ZookeeperBackgroundCleaner implements Runnable {
		
		
		private Logger logger = LoggerFactory.getLogger(ZookeeperBackgroundCleaner.class);
		
		
		private CuratorFramework  client;
		private static final long ZNODES_TTL = 600000; // 10 minutes 
		private static final long CLEAN_INTERVAL = 30000; // 5 minutes

		/**
		 * 
		 */
		public ZookeeperBackgroundCleaner(CuratorFramework  client) {
			this.client = client;
			logger.debug("Starting ZookeeperBackgroundCleaner...");
			logger.info("ZookeeperBackgroundCleaner BASE path " + STREAMING.ZK_BASE_PATH);
		}
		
		
		private int removeOldChildZnodes(String path) throws Exception {
			
			int counter = 0;
			Iterator<String> children = client.getChildren().forPath(path).iterator();
									
			while(children.hasNext()) {
				
				String childrenPath = children.next();								
				
				if (client.getChildren().forPath(path + "/" + childrenPath).size() > 0) {
					counter += removeOldChildZnodes(path + "/" + childrenPath);
				}
				else {
					
					Stat znode = client.checkExists().forPath(path + "/" + childrenPath);
//					avoid nulls and ephemeral znodes
					if (znode != null && znode.getEphemeralOwner() == 0) {
						client.delete().deletingChildrenIfNeeded().forPath(path + "/" + childrenPath);
						counter++;
					}					
					
				}									
			}
			
			return counter;
		}
		

		/* (non-Javadoc)
		 * @see java.lang.Runnable#run()
		 */
		@Override
		public void run() {
			
	
			while(!Thread.currentThread().isInterrupted()) {
				
				try {
					
					if (client.getState().compareTo(CuratorFrameworkState.STARTED) == 0) {
						int childsRemoved = removeOldChildZnodes(STREAMING.ZK_BASE_PATH);
						
						logger.debug(childsRemoved + " old zNodes removed from ZK");
					}
					
					
					Thread.sleep(CLEAN_INTERVAL);
					
					
				}
				catch(InterruptedException ie) {
//					no need to clean anything, as client is shared
					logger.info("Shutting down Zookeeper Background Cleaner");
				}
				
				catch (Exception e) {
					logger.info("Error on Zookeeper Background Cleaner: "+ e.getMessage());					
				}
				
				
			}

		}
		
		
		
		
	}

	
	
}
