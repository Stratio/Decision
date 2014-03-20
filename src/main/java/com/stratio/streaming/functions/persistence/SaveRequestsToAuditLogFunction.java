package com.stratio.streaming.functions.persistence;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;

import com.stratio.deep.config.DeepJobConfigFactory;
import com.stratio.deep.config.IDeepJobConfig;
import com.stratio.deep.entity.Cells;
import com.stratio.deep.rdd.CassandraCellRDD;
import com.stratio.streaming.functions.StratioStreamingBaseFunction;
import com.stratio.streaming.messages.BaseStreamingMessage;

public class SaveRequestsToAuditLogFunction extends StratioStreamingBaseFunction {
	
	private static Logger logger = LoggerFactory.getLogger(SaveRequestsToAuditLogFunction.class);
	private String cassandraCluster;
	

	/**
	 * 
	 */
	private static final long serialVersionUID = 7911766880059394316L;

	public SaveRequestsToAuditLogFunction(SiddhiManager siddhiManager, String zookeeperCluster, String kafkaCluster, String cassandraCluster) {
		super(siddhiManager, zookeeperCluster, kafkaCluster);
		this.cassandraCluster = cassandraCluster; 
	}
	

	@Override
	public Void call(JavaRDD<BaseStreamingMessage> rdd) throws Exception {
		
		
//		TODO when siddhi RDD is ready
				
//		final IDeepJobConfig<Cells> averageCassandraConfig = DeepJobConfigFactory.create().host("node.stratio.com").rpcPort(9160).keyspace("stratio_streaming").table("audit").initialize();
//		
//		JavaRDD<Cells> outRDD = rdd.map(new Function<BaseStreamingMessage, Cells>() {
//											@Override
//											public Cells call(BaseStreamingMessage request) {
//												
////												com.stratio.deep.entity.Cell<String> timestampCell = com.stratio.deep.entity.Cell.create("timestamp", request.);
////												com.stratio.deep.entity.Cell<UUID> sensorTimeUUIDCell = com.stratio.deep.entity.Cell.create("time_taken", UUIDGen.getTimeUUID(), true, false);
////												com.stratio.deep.entity.Cell<Double> sensorDataCell = com.stratio.deep.entity.Cell.create("value", tuple2._2());
//			
//												return null;//new Cells(sensorNameCell, sensorTimeUUIDCell, sensorDataCell);
//											}
//										});
//
//				
//		CassandraCellRDD.saveRDDToCassandra(outRDD, averageCassandraConfig);			

		return null;
	}

}
