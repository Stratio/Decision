/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
///**
// * Copyright (C) 2014 Stratio (http://stratio.com)
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *         http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package com.stratio.streaming.functions.requests;
//
//import org.apache.cassandra.utils.UUIDGen;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.function.Function;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import com.datastax.driver.core.Cluster;
//import com.datastax.driver.core.Cluster.Builder;
//import com.datastax.driver.core.Session;
//import com.stratio.deep.config.DeepJobConfigFactory;
//import com.stratio.deep.config.ICassandraDeepJobConfig;
//import com.stratio.deep.entity.Cells;
//import com.stratio.deep.rdd.CassandraCellRDD;
//import com.stratio.streaming.commons.constants.STREAMING;
//import com.stratio.streaming.commons.messages.StratioStreamingMessage;
//
//public class SaveRequestsToAuditLogFunction implements Function<JavaRDD<StratioStreamingMessage>, Void> {
//
//    private static Logger logger = LoggerFactory.getLogger(SaveRequestsToAuditLogFunction.class);
//    private ICassandraDeepJobConfig<Cells> auditCassandraConfig;
//    private static final String AUDITING_TABLE = "auditing_requests";
//
//    /**
//	 * 
//	 */
//    private static final long serialVersionUID = 7911766880059394316L;
//
//    public SaveRequestsToAuditLogFunction(String cassandraCluster) {
//        try {
//            Builder cBuilder = new Cluster.Builder();
//
//            for (String node : cassandraCluster.split(",")) {
//                // Add data center to Cassandra cluster
//                cBuilder.addContactPoint(node);
//            }
//            Cluster cCluster = cBuilder.build();
//            Session cassandraSession = cCluster.connect();
//
//            // if
//            // (cassandraSession.getCluster().getMetadata().getKeyspace(STREAMING.STREAMING_KEYSPACE_NAME)
//            // == null) {
//            // cassandraSession.execute(STREAMING.CREATE_STREAMING_KEYSPACE);
//            // }
//
//            this.auditCassandraConfig = DeepJobConfigFactory.createWriteConfig().createTableOnWrite(true)
//                    .keyspace(STREAMING.STREAMING_KEYSPACE_NAME).table(AUDITING_TABLE).rpcPort(9160)
//                    .host(cassandraCluster);
//
//            cassandraSession.close();
//            cCluster.close();
//        } catch (Exception e) {
//            logger.error("Auditing service can not be started. Reason: " + e.getMessage() + "//" + e.getClass());
//        }
//
//        if (auditCassandraConfig != null) {
//            auditCassandraConfig.initialize();
//        }
//    }
//
//    @Override
//    public Void call(JavaRDD<StratioStreamingMessage> rdd) throws Exception {
//
//
//        if (rdd.count() > 0 && auditCassandraConfig != null) {
//
//            JavaRDD<Cells> outRDD = rdd.map(new Function<StratioStreamingMessage, Cells>() {
//                @Override
//                public Cells call(StratioStreamingMessage request) {
//
//                    com.stratio.deep.entity.Cell requestTimeUUIDCell = com.stratio.deep.entity.Cell.create(
//                            "time_taken", UUIDGen.getTimeUUID(), true, false);
//                    com.stratio.deep.entity.Cell requestSessionIdCell = com.stratio.deep.entity.Cell.create(
//                            "sessionId", request.getSession_id());
//                    com.stratio.deep.entity.Cell requestIdCell = com.stratio.deep.entity.Cell.create("requestId",
//                            request.getRequest_id());
//                    com.stratio.deep.entity.Cell requestStreamNameCell = com.stratio.deep.entity.Cell.create(
//                            "streamName", request.getStreamName());
//                    com.stratio.deep.entity.Cell requestOperationNameCell = com.stratio.deep.entity.Cell.create(
//                            "operation", request.getOperation());
//                    com.stratio.deep.entity.Cell requestDataCell = com.stratio.deep.entity.Cell.create("request",
//                            request.getRequest());
//
//                    return new Cells(requestTimeUUIDCell, requestSessionIdCell, requestIdCell, requestStreamNameCell,
//                            requestOperationNameCell, requestDataCell);
//                }
//            });
//            CassandraCellRDD.cql3SaveRDDToCassandra(outRDD.rdd(), auditCassandraConfig);
//        }
//
//        return null;
//    }
//
// }
