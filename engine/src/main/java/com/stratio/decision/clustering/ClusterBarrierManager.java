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
