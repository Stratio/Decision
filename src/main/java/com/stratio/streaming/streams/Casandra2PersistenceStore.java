/*
*  Copyright (c) 2005-2012, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package com.stratio.streaming.streams;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import org.apache.cassandra.utils.UUIDGen;
import org.apache.log4j.Logger;
import org.wso2.siddhi.core.event.management.PersistenceManagementEvent;
import org.wso2.siddhi.core.persistence.ByteSerializer;
import org.wso2.siddhi.core.persistence.PersistenceObject;
import org.wso2.siddhi.core.persistence.PersistenceStore;





public class Casandra2PersistenceStore implements PersistenceStore {
	private static Logger log = Logger.getLogger(Casandra2PersistenceStore.class);
    

    

    private static final String KEYSPACE 							= "StreamingSnapshots";
    private static final String COLUMN_FAMILY_NAME 					= "Snapshots";
    private static final String INDEX_COLUMN_FAMILY_NAME 			= "SnapshotsIndex";
    private static final String EXECUTION_PLAN_COLUMN_FAMILY_NAME 	= "ExecutionPlanSnapshots";

    private StringSerializer sser = new StringSerializer();
    private BytesArraySerializer bser = new BytesArraySerializer();
    
    private Keyspace keyspace;
    private Cluster cluster;

    private static Date timeAt1970 = new Date(10000);


    
    
    public Casandra2PersistenceStore(String cassandraCluster, String username, String password) {

 
        Map<String, String> credentials = new HashMap<String, String>();
        credentials.put("username", username);
        credentials.put("password", password);
        cluster = HFactory.createCluster("TestCluster", new CassandraHostConfigurator(cassandraCluster), credentials);

        init(cluster);

    }

    private void init(Cluster cluster) {
        if (cluster.describeKeyspace(KEYSPACE) == null) {
            log.info("Creating  keyspace for snapshosts " + KEYSPACE);
            cluster.addKeyspace(HFactory.createKeyspaceDefinition(KEYSPACE));
            keyspace = HFactory.createKeyspace(KEYSPACE, cluster);
            cluster.addColumnFamily(HFactory.createColumnFamilyDefinition(keyspace.getKeyspaceName(), COLUMN_FAMILY_NAME));
            cluster.addColumnFamily(HFactory.createColumnFamilyDefinition(keyspace.getKeyspaceName(), INDEX_COLUMN_FAMILY_NAME));
            cluster.addColumnFamily(HFactory.createColumnFamilyDefinition(keyspace.getKeyspaceName(), EXECUTION_PLAN_COLUMN_FAMILY_NAME));

        } else {
            if (log.isDebugEnabled()) {
                log.debug("keyspace " + KEYSPACE + " exists");
            }
            keyspace = HFactory.createKeyspace(KEYSPACE, cluster);
        }
    }

    public Casandra2PersistenceStore(Cluster cluster) {
        init(cluster);
    }
    

    public void save(PersistenceManagementEvent persistenceManagementEvent, String nodeID, PersistenceObject persistenceObject) {
    	
        Mutator<String> mutator = HFactory.createMutator(keyspace, sser);
        
        mutator.insert(persistenceManagementEvent.getRevision(), 
        					COLUMN_FAMILY_NAME, 
        					HFactory.createColumn(nodeID, ByteSerializer.OToB(persistenceObject), sser, bser));
        
        mutator.insert(persistenceManagementEvent.getExecutionPlanIdentifier(), 
        				INDEX_COLUMN_FAMILY_NAME,
        				HFactory.createColumn(persistenceManagementEvent.getRevision(), String.valueOf(System.currentTimeMillis()), sser, sser));
        mutator.execute();
    }

    public PersistenceObject load(PersistenceManagementEvent persistenceManagementEvent,
                                  String nodeId) {

        List<NodeSnapshot> list = new ArrayList<NodeSnapshot>();

        ColumnSlice<String, byte[]> cs;

        SliceQuery<String, String, byte[]> q = HFactory.createSliceQuery(keyspace, sser, sser, bser);
        q.setColumnFamily(COLUMN_FAMILY_NAME).setKey(persistenceManagementEvent.getRevision()).setRange("", "", false, 1000).setColumnNames(nodeId);

        QueryResult<ColumnSlice<String, byte[]>> r = q.execute();

        cs = r.get();
        PersistenceObject persistenceObject = null;
        for (HColumn<String, byte[]> hc : cs.getColumns()) {
            persistenceObject = (PersistenceObject) ByteSerializer.BToO(hc.getValue());
//            list.add(new NodeSnapshot(hc.getName(), hc.getValue()));
        }
//        return list;
        return persistenceObject;

    }

    public String getLastRevision(String executionPlanIdentifier) {
        ColumnSlice<String, byte[]> cs;
        String rangeStart = new StringBuffer(String.valueOf(timeAt1970.getTime())).append("_").toString();
        boolean firstLoop = true;
        while (true) {
            SliceQuery<String, String, byte[]> q = HFactory.createSliceQuery(keyspace, sser, sser, bser);
            q.setColumnFamily(INDEX_COLUMN_FAMILY_NAME).setKey(executionPlanIdentifier)
                    .setRange(rangeStart, String.valueOf(Long.MAX_VALUE), false, 1000);

            QueryResult<ColumnSlice<String, byte[]>> r = q.execute();

            cs = r.get();
            int size = cs.getColumns().size();
            if (firstLoop && size == 0) {
                return null;
            } else if (size == 0) {
                return rangeStart;
            } else {
                firstLoop = false;
            }
            int lastIndex = size - 1;
            rangeStart = cs.getColumns().get(lastIndex).getName();
            if (size < 1000) {
                break;
            }
        }
        log.debug("found revision " + rangeStart);
        return rangeStart;
    }
    
    
    public void saveExecutionPlan(String executionPlan) {
    	Mutator<String> mutator = HFactory.createMutator(keyspace, sser);
    	
    	
    	mutator.addInsertion("savedExecutionPlan", 
				    			EXECUTION_PLAN_COLUMN_FAMILY_NAME,
				    			HFactory.createColumn("executionPlan", executionPlan, StringSerializer.get(), StringSerializer.get()));
    			
        
    	mutator.execute();
    }
    
    
    public String recoverLastExecutionPlan(String executionPlanIdentifier) {
    	
    	String executionPlan = "";

	    	
	    	
	
		SliceQuery<String, String, String> q = HFactory.createSliceQuery(keyspace, sser, sser, sser);
		q.setColumnFamily(EXECUTION_PLAN_COLUMN_FAMILY_NAME).setKey("savedExecutionPlan").setRange("", "", false, 1000).setColumnNames("executionPlan");
		
		QueryResult<ColumnSlice<String, String>> r = q.execute();
								
		if (r.get().getColumnByName("executionPlan") != null) {
			executionPlan = r.get().getColumnByName("executionPlan").getValue();
		}

    	return executionPlan;

    }
    
    public void removeAllRevisions() {
    	
    	if (cluster.describeKeyspace(KEYSPACE) != null) {
    		cluster.truncate(KEYSPACE, COLUMN_FAMILY_NAME);
        	cluster.truncate(KEYSPACE, INDEX_COLUMN_FAMILY_NAME);
        	cluster.truncate(KEYSPACE, EXECUTION_PLAN_COLUMN_FAMILY_NAME);
    	}
    	
    }
    


    public class NodeSnapshot {
        String nodeID;
        byte[] data;

        public NodeSnapshot(String nodeID, byte[] data) {
            super();
            this.nodeID = nodeID;
            this.data = data;
        }

        @Override
        public String toString() {
            return new StringBuffer().append(nodeID).append(",").append(new String(data)).toString();
        }
    }
    
    
    
}