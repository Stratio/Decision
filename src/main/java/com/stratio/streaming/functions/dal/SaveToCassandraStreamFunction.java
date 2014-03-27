package com.stratio.streaming.functions.dal;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;

import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.entity.Cells;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.functions.StratioStreamingBaseFunction;

public class SaveToCassandraStreamFunction extends StratioStreamingBaseFunction {
	
	private static Logger logger = LoggerFactory.getLogger(SaveToCassandraStreamFunction.class);
	private ConcurrentHashMap<String, IDeepJobConfig<Cells>> existingStreams = new ConcurrentHashMap<String, IDeepJobConfig<Cells>>();
	private String cassandraCluster;
	

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
			
////			TODO if requested operation is DROP, then stop all existingStreams shutdown
//			
////			stream exists
//			if (getSiddhiManager().getInputHandler(request.getStreamName()) != null) {
//				
//				if (existingStreams.isEmpty() || !existingStreams.contains(request.getStreamName())) {
//					
//					
//					try {
//						
//						IDeepJobConfig<Cells> bulkCassandraConfig =  DeepJobConfigFactory.create().host("node.stratio.com").rpcPort(9160).keyspace("stratio_streaming").table(request.getStreamName()).initialize();
//						
//						existingStreams.put(request.getStreamName(), bulkCassandraConfig);
//												
//						
//						logger.info("==> LISTEN: stream " + request.getStreamName() + " is now working OK");
//						
//						ackStreamingOperation(request, StratioStreamingConstants.REPLY_CODES.OK);
//					} catch (Exception e) {
//						ackStreamingOperation(request, StratioStreamingConstants.REPLY_CODES.KO_GENERAL_ERROR);
//					}
//					
//					
//				}
//				
//				
////				writeToCassandra
//				
//					
//			}
//			else {
//				logger.info("==> LISTEN: stream " + request.getStreamName() + " does not exist, no go to the LISTEN KO");
//				ackStreamingOperation(request, StratioStreamingConstants.REPLY_CODES.KO_STREAM_DOES_NOT_EXIST);
//			}
		}
		
		
		
		return null;
	}

}
