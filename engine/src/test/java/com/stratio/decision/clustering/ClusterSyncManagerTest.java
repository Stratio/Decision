package com.stratio.decision.clustering;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.contains;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;

import org.apache.curator.framework.imps.CuratorFrameworkImpl;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.reflect.internal.WhiteboxImpl;

import com.google.gson.Gson;
import com.stratio.decision.commons.constants.ReplyCode;
import com.stratio.decision.commons.constants.STREAMING;
import com.stratio.decision.commons.dto.ActionCallbackDto;
import com.stratio.decision.commons.messages.StratioStreamingMessage;
import com.stratio.decision.configuration.ConfigurationContext;
import com.stratio.decision.task.FailOverTask;
import com.stratio.decision.utils.ZKUtils;

/**
 * Created by josepablofernandez on 1/02/16.
 */
public class ClusterSyncManagerTest {


    private ClusterSyncManager clusterSyncManager;


    @Mock
    private ConfigurationContext configurationContext;

    @Mock
    private ZKUtils zkUtils;

    @Mock
    private FailOverTask failOverTask;

    @Mock
    private CuratorFrameworkImpl curatorFramework;

    @Mock
    private ClusterBarrierManager clusterBarrierManager;

    @Mock
    private StratioStreamingMessage message;

    @Mock
    private ActionCallbackDto nodeReply;

    @Before
    public  void setUp() throws Exception {

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

        curatorFramework = mock(CuratorFrameworkImpl.class, Mockito.RETURNS_DEEP_STUBS);

        zkUtils = mock(ZKUtils.class);
        failOverTask = mock(FailOverTask.class);
        clusterBarrierManager = mock(ClusterBarrierManager.class);

        message = mock(StratioStreamingMessage.class);
        when(message.getRequest_id()).thenReturn("111");
        when(message.getOperation()).thenReturn("create");

        nodeReply = mock(ActionCallbackDto.class);
        when(nodeReply.getErrorCode()).thenReturn(ReplyCode.OK.getCode());
        when(nodeReply.getDescription()).thenReturn(ReplyCode.OK.getMessage());

       clusterSyncManager = new ClusterSyncManager(STREAMING.ZK_CLUSTER_MANAGER_PATH, "id",
                configurationContext, failOverTask,curatorFramework, zkUtils, clusterBarrierManager);

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
                configurationContext, failOverTask,curatorFramework, zkUtils, clusterBarrierManager);

        doNothing().when(curatorFramework).start();
        when(curatorFramework.getZookeeperClient().blockUntilConnectedOrTimedOut()).thenReturn(true);


    }

    @Test
    public void testInitializedNodeStatusShouldBeGroupInitialize() throws Exception {

        when(configurationContext.isClusteringEnabled()).thenReturn(false);

        clusterSyncManager = new ClusterSyncManager(STREAMING.ZK_CLUSTER_MANAGER_PATH, "id",
                configurationContext, failOverTask,curatorFramework, zkUtils, clusterBarrierManager);

        String nodeStatusPath = clusterSyncManager.initializedNodeStatus();
        assertNotNull(nodeStatusPath);
        assertEquals(STREAMING.ZK_EPHEMERAL_NODE_STATUS_PATH,nodeStatusPath);

    }

    @Test
    public void testInitializedNodeStatusShouldBeStatusInitialize() throws Exception {

        String nodeStatusPath = clusterSyncManager.initializedNodeStatus();
        assertNotNull(nodeStatusPath);
        assertEquals(STREAMING.ZK_EPHEMERAL_GROUPS_STATUS_BASE_PATH.concat("/").concat(STREAMING.GROUPS_STATUS_BASE_PREFIX)
                .concat(configurationContext.getGroupId()), nodeStatusPath);

    }


    @Test
    public void testInitializedGroupStatusShouldBeInitilized() throws Exception {

        doNothing().when(zkUtils).createEphemeralZNode(anyString(), any(byte[].class));
        when(zkUtils.existZNode(contains("group1"))).thenReturn(true);
        when(zkUtils.existZNode(contains("group2"))).thenReturn(true);

        assertEquals( STREAMING.ZK_EPHEMERAL_NODE_STATUS_INITIALIZED, clusterSyncManager.initializedGroupStatus());

    }

    @Test
    public void testInitializedGroupStatusShouldBeGroupsDown() throws Exception {

        doNothing().when(zkUtils).createEphemeralZNode(anyString(), any(byte[].class));
        when(zkUtils.existZNode(contains("group1"))).thenReturn(true);
        when(zkUtils.existZNode(contains("group2"))).thenReturn(false);

        assertEquals(STREAMING.ZK_EPHEMERAL_NODE_STATUS_GROUPS_DOWN, clusterSyncManager.initializedGroupStatus()) ;

    }

    @Test
    public void testUpdateNodeStatusShouldBeInitialized() throws Exception {

        doNothing().when(zkUtils).createEphemeralZNode(anyString(), any(byte[].class));
        when(zkUtils.existZNode(contains("group1"))).thenReturn(false);
        when(zkUtils.existZNode(contains("group2"))).thenReturn(false);

        clusterSyncManager.initializedGroupStatus();
        clusterSyncManager.updateNodeStatus("group1", PathChildrenCacheEvent.Type.CHILD_ADDED);
        String status = clusterSyncManager.updateNodeStatus("group2", PathChildrenCacheEvent.Type.CHILD_ADDED);

        assertEquals(STREAMING.ZK_EPHEMERAL_NODE_STATUS_INITIALIZED, status);

    }


    @Test
    public void testUpdateNodeStatusShouldBeGroupsDown() throws Exception {

        doNothing().when(zkUtils).createEphemeralZNode(anyString(), any(byte[].class));
        when(zkUtils.existZNode(contains("group1"))).thenReturn(false);
        when(zkUtils.existZNode(contains("group2"))).thenReturn(false);

        clusterSyncManager.initializedGroupStatus();
        clusterSyncManager.updateNodeStatus("group1", PathChildrenCacheEvent.Type.CHILD_ADDED);
        clusterSyncManager.updateNodeStatus("group2", PathChildrenCacheEvent.Type.CHILD_ADDED);

        String status = clusterSyncManager.updateNodeStatus("group2", PathChildrenCacheEvent.Type.CHILD_REMOVED);

        assertEquals(STREAMING.ZK_EPHEMERAL_NODE_STATUS_GROUPS_DOWN, status);

    }


    @Test
    public void testManageAckStreamingOperationNoClustering() throws Exception {

        when(configurationContext.isClusteringEnabled()).thenReturn(false);

        final ClusterSyncManager partiallyMockedClusterSyncManager = PowerMockito.spy(new ClusterSyncManager(STREAMING
                .ZK_CLUSTER_MANAGER_PATH, "id", configurationContext, failOverTask, curatorFramework, zkUtils,
                clusterBarrierManager));


        doNothing().when(zkUtils).createZNodeJsonReply(any(StratioStreamingMessage.class), any(Object.class));

        ActionCallbackDto reply = partiallyMockedClusterSyncManager.manageAckStreamingOperation(message, nodeReply);
        assertEquals(nodeReply, reply);


    }

    @Test
    public void testManageAckStreamingOperationNoAck() throws Exception {

        final ClusterSyncManager spyClusterSyncManager = PowerMockito.spy(new ClusterSyncManager(STREAMING
                .ZK_CLUSTER_MANAGER_PATH, "id", configurationContext, failOverTask, curatorFramework, zkUtils,
                clusterBarrierManager));

        PowerMockito.doReturn(true).when(spyClusterSyncManager).isLeader();

        doNothing().when(zkUtils).createZNodeJsonReply(any(StratioStreamingMessage.class), any(Object.class));

        ActionCallbackDto reply = spyClusterSyncManager.manageAckStreamingOperation(message, nodeReply);
        assertEquals(nodeReply, reply);


    }

    @Test
    public void testManageAckStreamingOperationAck() throws Exception {

        final ClusterSyncManager spyClusterSyncManager = PowerMockito.spy(new ClusterSyncManager(STREAMING
                .ZK_CLUSTER_MANAGER_PATH, "id", configurationContext, failOverTask, curatorFramework, zkUtils,
                clusterBarrierManager));

        PowerMockito.doReturn(true).when(spyClusterSyncManager).isLeader();

        doNothing().when(zkUtils).createTempZNodeJsonReply(any(StratioStreamingMessage.class), any(Object.class), anyString());

        when(clusterBarrierManager.manageAckBarrier(anyString(), anyInt())).thenReturn(true);

        doNothing().when(zkUtils).createZNodeJsonReply(any(StratioStreamingMessage.class), any(Object.class));
        when(curatorFramework.delete().deletingChildrenIfNeeded().forPath(anyString())).thenReturn(null);

        ActionCallbackDto reply = spyClusterSyncManager.manageAckStreamingOperation(message, nodeReply);
        assertEquals(nodeReply, reply);

    }


    @Test
    public void testManageBarrierResultsShouldBeNodeNotReply() throws Exception {

        String barrierPath = "/stratio/decision/barrier";

        ActionCallbackDto reply = WhiteboxImpl.invokeMethod(clusterSyncManager, "manageBarrierResults", message,
                nodeReply, barrierPath, false);

        Assert.assertEquals(ReplyCode.KO_NODE_NOT_REPLY.getCode(), reply.getErrorCode());
    }

    @Test
    public void testManageBarrierResultsShouldBeKO() throws Exception {

        String barrierPath = "/stratio/decision/barrier";

        when(nodeReply.getErrorCode()).thenReturn(ReplyCode.KO_GENERAL_ERROR.getCode());
        when(nodeReply.getDescription()).thenReturn(ReplyCode.KO_GENERAL_ERROR.getMessage());

        ActionCallbackDto reply = WhiteboxImpl.invokeMethod(clusterSyncManager, "manageBarrierResults", message,
                nodeReply, barrierPath, true);

        Assert.assertEquals(ReplyCode.KO_GENERAL_ERROR.getCode(), reply.getErrorCode()) ;
    }

    @Test
    public void testManageBarrierResultsShouldBeOK() throws Exception {

        String barrierPath = "/stratio/decision/barrier";

        when(nodeReply.getErrorCode()).thenReturn(ReplyCode.KO_GENERAL_ERROR.getCode());
        when(nodeReply.getDescription()).thenReturn(ReplyCode.KO_GENERAL_ERROR.getMessage());

        ActionCallbackDto reply = WhiteboxImpl.invokeMethod(clusterSyncManager, "manageBarrierResults", message,
                nodeReply, barrierPath, true);

        Assert.assertEquals(ReplyCode.KO_GENERAL_ERROR.getCode(), reply.getErrorCode());
    }


    @Test
    public void testManageBarrierResultsChildShouldBeKO() throws Exception {

        String barrierPath = "/stratio/decision/barrier";

        ActionCallbackDto parsedResponse = new ActionCallbackDto();
        parsedResponse.setDescription(ReplyCode.KO_GENERAL_ERROR.getMessage());
        parsedResponse.setErrorCode(ReplyCode.KO_GENERAL_ERROR.getCode());

        byte[] data = new Gson().toJson(parsedResponse).getBytes();

        when(curatorFramework.getData().forPath(anyString())).thenReturn(data);

        final ClusterSyncManager spyClusterSyncManager = PowerMockito.spy(new ClusterSyncManager(STREAMING
                .ZK_CLUSTER_MANAGER_PATH, "id", configurationContext, failOverTask, curatorFramework, zkUtils,
                clusterBarrierManager));


        ActionCallbackDto reply = WhiteboxImpl.invokeMethod(spyClusterSyncManager, "manageBarrierResults", message,
                nodeReply, barrierPath, true);

        Assert.assertEquals(ReplyCode.KO_GENERAL_ERROR.getCode(), reply.getErrorCode());
    }


    @Test
    public void testManageBarrierResultsChildShouldBeOK() throws Exception {

        String barrierPath = "/stratio/decision/barrier";

        ActionCallbackDto parsedResponse = new ActionCallbackDto();
        parsedResponse.setDescription(ReplyCode.OK.getMessage());
        parsedResponse.setErrorCode(ReplyCode.OK.getCode());

        byte[] data = new Gson().toJson(parsedResponse).getBytes();

        when(curatorFramework.getData().forPath(anyString())).thenReturn(data);

        final ClusterSyncManager spyClusterSyncManager = PowerMockito.spy(new ClusterSyncManager(STREAMING
                .ZK_CLUSTER_MANAGER_PATH, "id", configurationContext, failOverTask, curatorFramework, zkUtils,
                clusterBarrierManager));


        ActionCallbackDto reply = WhiteboxImpl.invokeMethod(spyClusterSyncManager, "manageBarrierResults", message,
                nodeReply, barrierPath, true);

        Assert.assertEquals(ReplyCode.OK.getCode(), reply.getErrorCode());
    }


}
