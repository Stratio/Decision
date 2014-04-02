package com.stratio.streaming.functions.requests;

import java.util.UUID;

import org.apache.cassandra.utils.UUIDGen;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;

import com.stratio.deep.config.DeepJobConfigFactory;
import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.rdd.CassandraCellRDD;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.functions.StratioStreamingBaseFunction;

public class SaveRequestsToAuditLogFunction extends StratioStreamingBaseFunction {
	
	private static Logger logger = LoggerFactory.getLogger(SaveRequestsToAuditLogFunction.class);
	private String cassandraCluster;
	private IDeepJobConfig<Cells> auditCassandraConfig;
	

	/**
	 * 
	 */
	private static final long serialVersionUID = 7911766880059394316L;

	public SaveRequestsToAuditLogFunction(SiddhiManager siddhiManager, String zookeeperCluster, String kafkaCluster, String cassandraCluster, Boolean auditEnabled) {
		super(siddhiManager, zookeeperCluster, kafkaCluster);
		this.cassandraCluster = cassandraCluster;
		if (auditEnabled) {
			this.auditCassandraConfig = DeepJobConfigFactory.create().host("node.stratio.com").rpcPort(9160).keyspace("stratio_streaming").table("auditing_requests").createTableOnWrite(true).initialize();
		}
	}
	

	@Override
	public Void call(JavaRDD<StratioStreamingMessage> rdd) throws Exception {
		
		
//		TODO when siddhi RDD is ready
		
		if (rdd.count() > 0) {

			JavaRDD<Cells> outRDD = rdd.map(new Function<StratioStreamingMessage, Cells>() {
				@Override
				public Cells call(StratioStreamingMessage request) {
					
					com.stratio.deep.entity.Cell<UUID>   requestTimeUUIDCell 		= com.stratio.deep.entity.Cell.create("time_taken", UUIDGen.getTimeUUID(), true, false);
					com.stratio.deep.entity.Cell<String> requestSessionIdCell 		= com.stratio.deep.entity.Cell.create("sessionId", request.getSession_id());
					com.stratio.deep.entity.Cell<String> requestIdCell 				= com.stratio.deep.entity.Cell.create("requestId", request.getRequest_id());
					com.stratio.deep.entity.Cell<String> requestStreamNameCell 		= com.stratio.deep.entity.Cell.create("streamName", request.getStreamName());
					com.stratio.deep.entity.Cell<String> requestOperationNameCell 	= com.stratio.deep.entity.Cell.create("operation", request.getOperation());
					com.stratio.deep.entity.Cell<String> requestDataCell 			= com.stratio.deep.entity.Cell.create("request", request.getRequest());
	
					return new Cells(requestTimeUUIDCell, requestSessionIdCell, requestIdCell, requestStreamNameCell, requestOperationNameCell, requestDataCell);
				}
			});
	
	
			CassandraCellRDD.saveRDDToCassandra(outRDD, auditCassandraConfig);			
		}

		return null;
	}

}
