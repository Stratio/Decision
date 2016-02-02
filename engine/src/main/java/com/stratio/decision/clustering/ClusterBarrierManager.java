package com.stratio.decision.clustering;

import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;

/**
 * Created by josepablofernandez on 2/02/16.
 */
public class ClusterBarrierManager {

    private ClusterSyncManager clusterSyncManagerInstance;
    private Long ackTimeout;

    public ClusterBarrierManager(ClusterSyncManager clusterSyncManagerInstance, Long ackTimeOut){

        this.clusterSyncManagerInstance = clusterSyncManagerInstance;
        this.ackTimeout = ackTimeOut;

    }

    public Boolean manageAckBarrier(String barrierPath, Integer nodesExpected) throws Exception {

        DistributedDoubleBarrier barrier = new DistributedDoubleBarrier(clusterSyncManagerInstance.getClient(), barrierPath,
                nodesExpected);

        return barrier.enter(ackTimeout, TimeUnit.MILLISECONDS);

    }
}
