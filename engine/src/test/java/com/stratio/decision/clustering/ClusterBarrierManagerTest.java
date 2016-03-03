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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.imps.CuratorFrameworkImpl;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

/**
 * Created by josepablofernandez on 4/02/16.
 */
public class ClusterBarrierManagerTest {


    @Mock
    private CuratorFrameworkImpl curatorFramework;

    @Mock
    private DistributedDoubleBarrier barrier;

    @Before
    public  void setUp() throws Exception {

        curatorFramework = mock(CuratorFrameworkImpl.class, Mockito.RETURNS_DEEP_STUBS);
    }

    @Test
    public void testManageAckBarrierShouldBeOk() throws Exception {

        Integer ackTimeout = 500;
        String barrierPath = "/stratio/decision/barrier";
        Integer nodesExpected = 2;

        ClusterSyncManager clusterSyncManager = mock(ClusterSyncManager.class);
        when(clusterSyncManager.getClient()).thenReturn(curatorFramework);

        final ClusterBarrierManager clusterBarrierManager = PowerMockito.spy(new ClusterBarrierManager
                (clusterSyncManager, ackTimeout));

        barrier = mock(DistributedDoubleBarrier.class);
        when(barrier.enter(anyLong(), any(TimeUnit.class))).thenReturn(true);

        PowerMockito.doReturn(barrier).when(clusterBarrierManager).getDistributedDoubleBarrier(anyString(), anyInt());
        assertEquals(true, clusterBarrierManager.manageAckBarrier(barrierPath, nodesExpected));

    }

    @Test
    public void testManageAckBarrierShouldBeKO() throws Exception {

        Integer ackTimeout = 500;
        String barrierPath = "/stratio/decision/barrier";
        Integer nodesExpected = 2;

        ClusterSyncManager clusterSyncManager = mock(ClusterSyncManager.class);
        when(clusterSyncManager.getClient()).thenReturn(curatorFramework);

        final ClusterBarrierManager clusterBarrierManager = PowerMockito.spy(new ClusterBarrierManager
                (clusterSyncManager, ackTimeout));

        barrier = mock(DistributedDoubleBarrier.class);
        when(barrier.enter(anyLong(), any(TimeUnit.class))).thenReturn(false);

        PowerMockito.doReturn(barrier).when(clusterBarrierManager).getDistributedDoubleBarrier(anyString(), anyInt());
        assertEquals(false, clusterBarrierManager.manageAckBarrier(barrierPath, nodesExpected));

    }

}
