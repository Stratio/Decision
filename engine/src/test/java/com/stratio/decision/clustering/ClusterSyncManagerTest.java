package com.stratio.decision.clustering;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.anyVararg;
import static org.mockito.Matchers.contains;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkImpl;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.junit.Assert;

import com.stratio.decision.commons.constants.STREAMING;
import com.stratio.decision.configuration.ConfigurationContext;
import com.stratio.decision.task.FailOverTask;
import com.stratio.decision.utils.ZKUtils;

import static org.hamcrest.CoreMatchers.instanceOf;

/**
 * Created by josepablofernandez on 1/02/16.
 */
public class ClusterSyncManagerTest {


    @Mock
    ConfigurationContext configurationContext;

    @Mock
    CuratorFrameworkFactory curatorFrameworkFactory;

    @Mock
    ZKUtils zkUtils;

    @Mock
    FailOverTask failOverTask;

    @Mock
    CuratorFrameworkImpl curatorFramework;

    @Before
    public void setUp() throws Exception {

        configurationContext = mock(ConfigurationContext.class);
        when(configurationContext.getZookeeperHostsQuorum()).thenReturn("localhost:2181");
        when(configurationContext.isClusteringEnabled()).thenReturn(true);

        ArrayList clusterGroups = new ArrayList<>();
        clusterGroups.add("group1");
        clusterGroups.add("group2");
        when(configurationContext.getClusterGroups()).thenReturn(clusterGroups);
        when(configurationContext.getGroupId()).thenReturn("group1");
        when(configurationContext.isAllAckEnabled()).thenReturn(false);
        when(configurationContext.getAckTimeout()).thenReturn(500);

        curatorFrameworkFactory = mock(CuratorFrameworkFactory.class);
        curatorFramework = mock(CuratorFrameworkImpl.class, Mockito.RETURNS_DEEP_STUBS);

        zkUtils = mock(ZKUtils.class);
        failOverTask = mock(FailOverTask.class);

    }


    @Test
    public void testGetClusterSyncManager() throws Exception {

        ClusterSyncManager clusterSyncManager = ClusterSyncManager.getClusterSyncManager(configurationContext,
                failOverTask);

        assertNotNull(clusterSyncManager);
        assertThat(clusterSyncManager, instanceOf(ClusterSyncManager.class));

    }

    @Test
    public void testGetNode() throws Exception {

        ClusterSyncManager.getClusterSyncManager(configurationContext,
                failOverTask);

        ClusterSyncManager clusterSyncManager = ClusterSyncManager.getNode();

        assertNotNull(clusterSyncManager);
        assertThat(clusterSyncManager, instanceOf(ClusterSyncManager.class));

    }

    @Test
    public void testStart() throws InterruptedException {

        ClusterSyncManager clusterSyncManager = new ClusterSyncManager(STREAMING.ZK_CLUSTER_MANAGER_PATH, "id",
                configurationContext, failOverTask,curatorFramework, zkUtils);

        doNothing().when(curatorFramework).start();
        when(curatorFramework.getZookeeperClient().blockUntilConnectedOrTimedOut()).thenReturn(true);


    }

    @Test
    public void testInitializedNodeStatusShouldBeGroupInitialize() throws Exception {

        when(configurationContext.isClusteringEnabled()).thenReturn(false);

        ClusterSyncManager clusterSyncManager = new ClusterSyncManager(STREAMING.ZK_CLUSTER_MANAGER_PATH, "id",
                configurationContext, failOverTask,curatorFramework, zkUtils);

        String nodeStatusPath = clusterSyncManager.initializedNodeStatus();
        assertNotNull(nodeStatusPath);
        assertEquals(nodeStatusPath,  STREAMING.ZK_EPHEMERAL_NODE_STATUS_PATH);

    }

    @Test
    public void testInitializedNodeStatusShouldBeStatusInitialize() throws Exception {


        ClusterSyncManager clusterSyncManager = new ClusterSyncManager(STREAMING.ZK_CLUSTER_MANAGER_PATH, "id",
                configurationContext, failOverTask,curatorFramework, zkUtils);

        String nodeStatusPath = clusterSyncManager.initializedNodeStatus();
        assertNotNull(nodeStatusPath);
        assertEquals(nodeStatusPath,  STREAMING.ZK_EPHEMERAL_GROUPS_STATUS_BASE_PATH.concat("/").concat(STREAMING.GROUPS_STATUS_BASE_PREFIX)
                .concat(configurationContext.getGroupId()));

    }


    @Test
    public void testInitializedGroupStatusShouldBeInitilized() throws Exception {

        doNothing().when(zkUtils).createEphemeralZNode(anyString(), any(byte[].class));
        when(zkUtils.existZNode(contains("group1"))).thenReturn(true);
        when(zkUtils.existZNode(contains("group2"))).thenReturn(true);

        ClusterSyncManager clusterSyncManager = new ClusterSyncManager(STREAMING.ZK_CLUSTER_MANAGER_PATH, "id",
                configurationContext, failOverTask,curatorFramework, zkUtils);

        assertEquals(clusterSyncManager.initializedGroupStatus(), STREAMING.ZK_EPHEMERAL_NODE_STATUS_INITIALIZED);

    }

    @Test
    public void testInitializedGroupStatusShouldBeGroupsDown() throws Exception {

        doNothing().when(zkUtils).createEphemeralZNode(anyString(), any(byte[].class));
        when(zkUtils.existZNode(contains("group1"))).thenReturn(true);
        when(zkUtils.existZNode(contains("group2"))).thenReturn(false);

        ClusterSyncManager clusterSyncManager = new ClusterSyncManager(STREAMING.ZK_CLUSTER_MANAGER_PATH, "id",
                configurationContext, failOverTask,curatorFramework, zkUtils);

        assertEquals(clusterSyncManager.initializedGroupStatus(),STREAMING.ZK_EPHEMERAL_NODE_STATUS_GROUPS_DOWN);

    }

    @Test
    public void testUpdateNodeStatusShouldBeInitialized() throws Exception {

        doNothing().when(zkUtils).createEphemeralZNode(anyString(), any(byte[].class));
        when(zkUtils.existZNode(contains("group1"))).thenReturn(false);
        when(zkUtils.existZNode(contains("group2"))).thenReturn(false);

        ClusterSyncManager clusterSyncManager = new ClusterSyncManager(STREAMING.ZK_CLUSTER_MANAGER_PATH, "id",
                configurationContext, failOverTask,curatorFramework, zkUtils);

        clusterSyncManager.initializedGroupStatus();
        clusterSyncManager.updateNodeStatus("group1", PathChildrenCacheEvent.Type.CHILD_ADDED);
        String status = clusterSyncManager.updateNodeStatus("group2", PathChildrenCacheEvent.Type.CHILD_ADDED);

        assertEquals(status, STREAMING.ZK_EPHEMERAL_NODE_STATUS_INITIALIZED);

    }


    @Test
    public void testUpdateNodeStatusShouldBeGroupsDown() throws Exception {

        doNothing().when(zkUtils).createEphemeralZNode(anyString(), any(byte[].class));
        when(zkUtils.existZNode(contains("group1"))).thenReturn(false);
        when(zkUtils.existZNode(contains("group2"))).thenReturn(false);

        ClusterSyncManager clusterSyncManager = new ClusterSyncManager(STREAMING.ZK_CLUSTER_MANAGER_PATH, "id",
                configurationContext, failOverTask,curatorFramework, zkUtils);

        clusterSyncManager.initializedGroupStatus();
        clusterSyncManager.updateNodeStatus("group1", PathChildrenCacheEvent.Type.CHILD_ADDED);
        clusterSyncManager.updateNodeStatus("group2", PathChildrenCacheEvent.Type.CHILD_ADDED);

        String status = clusterSyncManager.updateNodeStatus("group2", PathChildrenCacheEvent.Type.CHILD_REMOVED);

        assertEquals(status, STREAMING.ZK_EPHEMERAL_NODE_STATUS_GROUPS_DOWN);

    }



}
