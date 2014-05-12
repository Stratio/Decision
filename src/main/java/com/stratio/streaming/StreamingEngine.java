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
package com.stratio.streaming;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import ca.zmatrix.cli.ParseCmd;

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.stratio.streaming.commons.constants.BUS;
import com.stratio.streaming.commons.constants.STREAMING;
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.functions.dal.ListenStreamFunction;
import com.stratio.streaming.functions.dal.SaveToCassandraStreamFunction;
import com.stratio.streaming.functions.dal.StopListenStreamFunction;
import com.stratio.streaming.functions.ddl.AddQueryToStreamFunction;
import com.stratio.streaming.functions.ddl.AlterStreamFunction;
import com.stratio.streaming.functions.ddl.CreateStreamFunction;
import com.stratio.streaming.functions.ddl.DropStreamFunction;
import com.stratio.streaming.functions.ddl.RemoveQueryToStreamFunction;
import com.stratio.streaming.functions.dml.InsertIntoStreamFunction;
import com.stratio.streaming.functions.dml.ListStreamsFunction;
import com.stratio.streaming.functions.messages.FilterMessagesByOperationFunction;
import com.stratio.streaming.functions.messages.KeepPayloadFromMessageFunction;
import com.stratio.streaming.functions.requests.CollectRequestForStatsFunction;
import com.stratio.streaming.functions.requests.SaveRequestsToAuditLogFunction;
import com.stratio.streaming.streams.StreamPersistence;
import com.stratio.streaming.streams.StreamSharedStatus;
import com.stratio.streaming.utils.SiddhiUtils;
import com.stratio.streaming.utils.ZKUtils;



/**
 * @author dmorales
 * 
 * =================
 * Stratio Streaming
 * =================
 * 
 * 1) Run a global Siddhi CEP engine
 * 2) Listen to the Kafka topic in order to receive stream commands (CREATE, ADDQUERY, LIST, DROP, INSERT, LISTEN, ALTER)
 * 3) Execute commands and send ACKs to Zookeeper
 * 4) Send back the events if there are listeners 
 *
 */
public class StreamingEngine {
	
	
	private static Logger logger = LoggerFactory.getLogger(StreamingEngine.class);
	private static SiddhiManager siddhiManager;
	private static String cassandraCluster;
	private static Boolean failOverEnabled;
	private static JavaStreamingContext jssc;

	/**
	 * 
	 * usage: --sparkMaster local --zookeeper-cluster fqdn:port --kafka-cluster fqdn:port
	 * 
	 * 
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) {
		
//		DECODING ARGUMENTS FROM COMMAND LINE
		String usage = "usage: --sparkMaster local --zookeeper-cluster fqdn:port,fqdn2:port --kafka-cluster fqdn:port,fqdn2:port --cassandra-cluster fqdn,fqdn2";
        ParseCmd cmd = new ParseCmd.Builder()
        							.help(usage)                          
        							.parm("--spark-master", 		"local").req()
//        							.parm("--sparkMaster", 			"node.stratio.com:9999" ).rex("^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\\-]*[a-zA-Z0-9])\\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\\-]*[A-Za-z0-9]):[0-9]{1,4}$").req()
        							.parm("--zookeeper-cluster", 	"node.stratio.com:2181").rex("^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\\-]*[a-zA-Z0-9])\\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\\-]*[A-Za-z0-9]):[0-9]{1,4}+(,(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\\-]*[a-zA-Z0-9])\\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\\-]*[A-Za-z0-9]):[0-9]{1,4}+)*$").req()
        							.parm("--kafka-cluster", 		"node.stratio.com:9092").rex("^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\\-]*[a-zA-Z0-9])\\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\\-]*[A-Za-z0-9]):[0-9]{1,4}+(,(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\\-]*[a-zA-Z0-9])\\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\\-]*[A-Za-z0-9]):[0-9]{1,4}+)*$").req()
        							.parm("--cassandra-cluster", 	"node.stratio.com").rex("^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\\-]*[a-zA-Z0-9])\\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\\-]*[A-Za-z0-9])+(,(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\\-]*[a-zA-Z0-9])\\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\\-]*[A-Za-z0-9])+)*$")
        							.parm("--auditEnabled", 		"false")
        							.parm("--statsEnabled", 		"false")
        							.parm("--failOverEnabled", 		"false")
        							.parm("--printStreams", 		"false")
        							.build();  
       
        HashMap<String, String> R = new HashMap<String,String>();
        String parseError    = cmd.validate(args);
        if( cmd.isValid(args) ) {
            R = (HashMap<String, String>) cmd.parse(args);            
            logger.info("Settings received:" + cmd.displayMap(R));
        }
        else { 
        	logger.error(parseError); 
        	System.exit(1); 
        }
        
        
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
            	
            	logger.info("Shutting down Stratio Streaming..");
            	
//            	shutdown spark
            	if (jssc != null) {
            		jssc.stop();
            	}

//            	shutdown siddhi
            	if (siddhiManager != null) {
            		
//            		remove All revisions (HA)
            		StreamPersistence.removeEngineStatusFromCleanExit(getSiddhiManager());
            		
//                	shutdown listeners
	        			
        			try {
        			
        				getSiddhiManager().getSiddhiContext().getHazelcastInstance().getTopic(STREAMING.INTERNAL_LISTEN_TOPIC).publish("*");
        				getSiddhiManager().getSiddhiContext().getHazelcastInstance().getTopic(STREAMING.INTERNAL_SAVE2CASSANDRA_TOPIC).publish("*");
            		
        			}
        			catch(HazelcastInstanceNotActiveException notActive) {
        				logger.info("Hazelcast is not active at this moment");
        			}
        	

        			getSiddhiManager().shutdown();
            	}        	

            	
//            	shutdown zookeeper
            	ZKUtils.shutdownZKUtils();
            	
            	
            	logger.info("Shutdown complete, bye�");
            }
        });
        
        
        try {
			launchStratioStreamingEngine(R.get("--spark-master").toString(),        		
											R.get("--zookeeper-cluster").toString(),
											R.get("--kafka-cluster").toString(),
											BUS.TOPICS,
											R.get("--cassandra-cluster").toString(),
											Boolean.valueOf(R.get("--auditEnabled").toString()),
											Boolean.valueOf(R.get("--statsEnabled").toString()),
											Boolean.valueOf(R.get("--failOverEnabled").toString()),
											Boolean.valueOf(R.get("--printStreams").toString()));
		} catch (Exception e) {
			logger.error("General error: " + e.getMessage() + " // " + e.getClass(),e);
		}

	}
	
	
	/**
	 * 
	 * - Launch the main process: spark context, kafkaInputDstream and siddhi CEP engine
	 * - Filter the incoming messages (kafka) by key in order to separate the different commands
	 * - Parse the request contained in the payload
	 * - execute related command for each request
	 *  
	 * 
	 * 
	 * @param sparkMaster
	 * @param zkCluster
	 * @param kafkaCluster
	 * @param topics
	 * @throws Exception 
	 */
	private static void launchStratioStreamingEngine(String sparkMaster, 
														String zkCluster, 
														String kafkaCluster, 
														String topics, 
														String cassandraClusterParam, 
														Boolean enableAuditing, 
														Boolean enableStats,
														Boolean failOverEnabledParam,
														Boolean printStreams) throws Exception {
		
		cassandraCluster = cassandraClusterParam;
		failOverEnabled = failOverEnabledParam;
		

		ZKUtils.getZKUtils(zkCluster).createEphemeralZNode(STREAMING.ZK_BASE_PATH + "/" + "engine", String.valueOf(System.currentTimeMillis()).getBytes());
		
//		Create the context with a x seconds batch size
//		jssc = new JavaStreamingContext(sparkMaster, StreamingEngine.class.getName(), 
//																new Duration(STREAMING.DURATION_MS), System.getenv("SPARK_HOME"), 
//																JavaStreamingContext.jarOfClass(StreamingEngine.class));
		jssc = new JavaStreamingContext("local[2]",
				StreamingEngine.class.getName(), new Duration(
						STREAMING.DURATION_MS));
		jssc.sparkContext().getConf().setJars(JavaStreamingContext.jarOfClass(StreamingEngine.class));
		
		KeepPayloadFromMessageFunction keepPayloadFromMessageFunction = new KeepPayloadFromMessageFunction();
		CreateStreamFunction createStreamFunction = new CreateStreamFunction(getSiddhiManager(), zkCluster, kafkaCluster);
		AlterStreamFunction alterStreamFunction = new AlterStreamFunction(getSiddhiManager(), zkCluster, kafkaCluster);
		InsertIntoStreamFunction insertIntoStreamFunction = new InsertIntoStreamFunction(getSiddhiManager(), zkCluster, kafkaCluster);
		AddQueryToStreamFunction addQueryToStreamFunction = new AddQueryToStreamFunction(getSiddhiManager(), zkCluster, kafkaCluster);
		RemoveQueryToStreamFunction removeQueryToStreamFunction = new RemoveQueryToStreamFunction(getSiddhiManager(), zkCluster, kafkaCluster);
		ListenStreamFunction listenStreamFunction = new ListenStreamFunction(getSiddhiManager(), zkCluster, kafkaCluster);
		CollectRequestForStatsFunction collectRequestForStatsFunction = new CollectRequestForStatsFunction(getSiddhiManager(), zkCluster, kafkaCluster);
		ListStreamsFunction listStreamsFunction = new ListStreamsFunction(getSiddhiManager(), zkCluster, kafkaCluster);
		DropStreamFunction dropStreamFunction = new DropStreamFunction(getSiddhiManager(), zkCluster, kafkaCluster);
		SaveRequestsToAuditLogFunction saveRequestsToAuditLogFunction = new SaveRequestsToAuditLogFunction(getSiddhiManager(), zkCluster, kafkaCluster, cassandraCluster, enableAuditing);
		StopListenStreamFunction stopListenStreamFunction = new StopListenStreamFunction(getSiddhiManager(), zkCluster, kafkaCluster);
		SaveToCassandraStreamFunction saveToCassandraStreamFunction = new SaveToCassandraStreamFunction(getSiddhiManager(), zkCluster, kafkaCluster, cassandraCluster);
		
		
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		String[] topic_list = topics.split(",");

		
//		building the topic map, by using the num of partitions of each topic
		for (String topic : topic_list) {
//			TODO use stratio bus API (int numberOfThreads = KafkaTopicUtils.getNumPartitionsForTopic(zkCluster, 9092, topic);)
			topicMap.put(topic, 2);
		}
				
		
//		Start the Kafka stream  
		JavaPairDStream<String, String> messages = KafkaUtils.createStream(jssc, zkCluster, BUS.STREAMING_GROUP_ID, topicMap);
		

//		as we are using messages several times, the best option is to cache it
		messages.cache();
		
		
//		Create a DStream for each command, so we can treat all related requests in the same way and also apply functions by command  
		JavaDStream<StratioStreamingMessage> create_requests = messages.filter(new FilterMessagesByOperationFunction(STREAM_OPERATIONS.DEFINITION.CREATE))
																  	.map(keepPayloadFromMessageFunction);
		
		JavaDStream<StratioStreamingMessage> alter_requests = messages.filter(new FilterMessagesByOperationFunction(STREAM_OPERATIONS.DEFINITION.ALTER))
			  														.map(keepPayloadFromMessageFunction);
		
		JavaDStream<StratioStreamingMessage> insert_requests = messages.filter(new FilterMessagesByOperationFunction(STREAM_OPERATIONS.MANIPULATION.INSERT))
																  	.map(keepPayloadFromMessageFunction);
		
		JavaDStream<StratioStreamingMessage> add_query_requests = messages.filter(new FilterMessagesByOperationFunction(STREAM_OPERATIONS.DEFINITION.ADD_QUERY))
				  												  	.map(keepPayloadFromMessageFunction);
		
		JavaDStream<StratioStreamingMessage> remove_query_requests = messages.filter(new FilterMessagesByOperationFunction(STREAM_OPERATIONS.DEFINITION.REMOVE_QUERY))
				  													.map(keepPayloadFromMessageFunction);
		
		JavaDStream<StratioStreamingMessage> listen_requests = messages.filter(new FilterMessagesByOperationFunction(STREAM_OPERATIONS.ACTION.LISTEN))
				  													.map(keepPayloadFromMessageFunction);
		
		JavaDStream<StratioStreamingMessage> stop_listen_requests = messages.filter(new FilterMessagesByOperationFunction(STREAM_OPERATIONS.ACTION.STOP_LISTEN))
																	.map(keepPayloadFromMessageFunction);
		
		JavaDStream<StratioStreamingMessage> saveToCassandra_requests = messages.filter(new FilterMessagesByOperationFunction(STREAM_OPERATIONS.ACTION.SAVETO_CASSANDRA))
																	.map(keepPayloadFromMessageFunction);
		
		
		JavaDStream<StratioStreamingMessage> list_requests = messages.filter(new FilterMessagesByOperationFunction(STREAM_OPERATIONS.MANIPULATION.LIST))
																	.map(keepPayloadFromMessageFunction);
		
		JavaDStream<StratioStreamingMessage> drop_requests = messages.filter(new FilterMessagesByOperationFunction(STREAM_OPERATIONS.DEFINITION.DROP))
				  													.map(keepPayloadFromMessageFunction);
		
		
		
		create_requests.foreachRDD(createStreamFunction);
		
		alter_requests.foreachRDD(alterStreamFunction);
		
		insert_requests.foreachRDD(insertIntoStreamFunction);
		
		add_query_requests.foreachRDD(addQueryToStreamFunction);
		
		remove_query_requests.foreachRDD(removeQueryToStreamFunction);
		
		listen_requests.foreachRDD(listenStreamFunction);
		
		stop_listen_requests.foreachRDD(stopListenStreamFunction);	

		saveToCassandra_requests.foreachRDD(saveToCassandraStreamFunction);
	
		list_requests.foreachRDD(listStreamsFunction);
		
		drop_requests.foreachRDD(dropStreamFunction);	
		
		
		if (enableAuditing || enableStats) {
			
			JavaDStream<StratioStreamingMessage> allRequests = create_requests
																.union(alter_requests)
																.union(insert_requests)
																.union(add_query_requests)
																.union(remove_query_requests)
																.union(listen_requests)
																.union(stop_listen_requests)
																.union(saveToCassandra_requests)
																.union(list_requests)
																.union(drop_requests);
			
			
			if (enableAuditing) {
//				persist the RDDs to cassandra using STRATIO DEEP
				allRequests.foreachRDD(saveRequestsToAuditLogFunction);				
			}
			
			if (enableStats) {
				allRequests.foreachRDD(collectRequestForStatsFunction);				
				
			}			
		}
		
							
	
		StreamPersistence.saveStreamingEngineStatus(getSiddhiManager());
		
		
		if (printStreams) {
			
		
	//		DEBUG STRATIO STREAMING ENGINE //
			messages.count().foreach(new Function<JavaRDD<Long>, Void>() {
	
				@Override
				public Void call(JavaRDD<Long> arg0) throws Exception {
					
					logger.info("********************************************");						
					logger.info("**       SIDDHI STREAMS                   **");
					logger.info("** countSiddhi:" + siddhiManager.getStreamDefinitions().size() + " // countHazelcast: " + getSiddhiManager().getSiddhiContext().getHazelcastInstance().getMap(STREAMING.STREAM_STATUS_MAP).size());											
					
					for(StreamDefinition streamMetaData : getSiddhiManager().getStreamDefinitions()) {
						
						StringBuffer streamDefinition = new StringBuffer();
						
						streamDefinition.append(streamMetaData.getStreamId());												
						
						for (Attribute column : streamMetaData.getAttributeList()) {
							streamDefinition.append(" |" + column.getName() + "," + column.getType());
						}
						
						if (StreamSharedStatus.getStreamStatus(streamMetaData.getStreamId(), getSiddhiManager()) != null) {
							HashMap<String, String> attachedQueries = StreamSharedStatus.getStreamStatus(streamMetaData.getStreamId(), getSiddhiManager()).getAddedQueries();
							
							streamDefinition.append(" /// " + attachedQueries.size() + " attachedQueries: (");
							
							for (String queryId : attachedQueries.keySet()) {
								streamDefinition.append(queryId + "/");
							}
							
							streamDefinition.append(" - userDefined:" + StreamSharedStatus.getStreamStatus(streamMetaData.getStreamId(), getSiddhiManager()).isUserDefined() + "- ");
							streamDefinition.append(" - listenEnable:" + StreamSharedStatus.getStreamStatus(streamMetaData.getStreamId(), getSiddhiManager()).isListen_enabled() + "- ");
						}
						
						logger.info("** stream: " + streamDefinition);
					}
					
					logger.info("********************************************");
					
					StreamPersistence.saveStreamingEngineStatus(getSiddhiManager());
					
					return null;
				}
				
			});
			
			
			
			messages.print();
			
		}
		
		jssc.start();
		jssc.awaitTermination();
		

	}
	
	private static SiddhiManager getSiddhiManager() {
		if (siddhiManager == null) {
			siddhiManager = SiddhiUtils.setupSiddhiManager(cassandraCluster, failOverEnabled);
		}
		
		return siddhiManager;
	}

}
