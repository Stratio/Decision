package com.stratio.decision.clustering;

import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;

/**
 * Created by josepablofernandez on 2/02/16.
 */
public class ClusterBarrierManager {

    private ClusterSyncManager clusterSyncManagerInstance;
    private Integer ackTimeout;

    public ClusterBarrierManager(ClusterSyncManager clusterSyncManagerInstance, Integer ackTimeOut){

        this.clusterSyncManagerInstance = clusterSyncManagerInstance;
        this.ackTimeout = ackTimeOut;

    }

    public Boolean manageAckBarrier(String barrierPath, Integer nodesExpected) throws Exception {

        DistributedDoubleBarrier barrier =  getDistributedDoubleBarrier(barrierPath, nodesExpected);
        return barrier.enter(ackTimeout, TimeUnit.MILLISECONDS);

    }

    public DistributedDoubleBarrier getDistributedDoubleBarrier(String barrierPath, Integer nodesExpected) {

        return new DistributedDoubleBarrier(clusterSyncManagerInstance.getClient(), barrierPath,
                nodesExpected);

    }
}
