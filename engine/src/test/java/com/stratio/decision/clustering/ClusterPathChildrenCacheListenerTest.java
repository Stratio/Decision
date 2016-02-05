package com.stratio.decision.clustering;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import com.stratio.decision.commons.constants.STREAMING;

/**
 * Created by josepablofernandez on 2/02/16.
 */
public class ClusterPathChildrenCacheListenerTest {

    @Mock
    ClusterSyncManager clusterSyncManager;

    @Before
    public void setUp() throws Exception {

        clusterSyncManager = mock(ClusterSyncManager.class);
        when(clusterSyncManager.updateNodeStatus(anyString(), any(PathChildrenCacheEvent.Type.class))).thenReturn(
                STREAMING.ZK_EPHEMERAL_NODE_STATUS_INITIALIZED);

    }

    @Test
    public void childEventTest() throws Exception {

        ClusterPathChildrenCacheListener listener = new ClusterPathChildrenCacheListener(clusterSyncManager);

        CuratorFramework client = mock(CuratorFramework.class);
        PathChildrenCacheEvent event = mock(PathChildrenCacheEvent.class, Mockito.RETURNS_DEEP_STUBS);

        when(event.getData().getPath()).thenReturn("/stratio/decision/group_group1");

        listener.childEvent(client, event);
    }




}
