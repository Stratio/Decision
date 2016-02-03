package com.stratio.decision.clustering;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.anyVararg;
import static org.mockito.Matchers.contains;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;

import org.apache.commons.math3.ode.events.EventHandler;
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

import static org.hamcrest.CoreMatchers.instanceOf;

/**
 * Created by josepablofernandez on 1/02/16.
 */
public class ClusterSyncManagerTest {


    private ClusterSyncManager clusterSyncManager;


    @Mock
    private ConfigurationContext configurationContext;

    @Mock
    private CuratorFrameworkFactory curatorFrameworkFactory;

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

//        ActionCallbackDto barrierReply = mock(ActionCallbackDto.class);
//        when(barrierReply.getErrorCode()).thenReturn(2);
//        when(barrierReply.getDescription()).thenReturn("KO");

//        PowerMockito.when(spyClusterSyncManager, PowerMockito.method(ClusterSyncManager.class, "manageBarrierResults", StratioStreamingMessage
//                .class, ActionCallbackDto.class, String.class, Boolean.class))
//                .withArguments(any(StratioStreamingMessage.class), any(ActionCallbackDto.class), anyString(),
//                        anyBoolean())
//                .thenReturn(barrierReply);



//        PowerMockito.doReturn(barrierReply).when(spyClusterSyncManager, "manageBarrierResults",  any
//                (StratioStreamingMessage.class), any(ActionCallbackDto.class), anyString(),
//           anyBoolean());

        doNothing().when(zkUtils).createZNodeJsonReply(any(StratioStreamingMessage.class), any(Object.class));
        when(curatorFramework.delete().deletingChildrenIfNeeded().forPath(anyString())).thenReturn(null);

        ActionCallbackDto reply = spyClusterSyncManager.manageAckStreamingOperation(message, nodeReply);
        assertEquals(nodeReply, reply);

    }


    @Test
    public void testManageBarrierResultsShouldBeNodeNotReply() throws Exception {

        ActionCallbackDto reply = WhiteboxImpl.invokeMethod(clusterSyncManager, "manageBarrierResults", message,
                nodeReply, "/stratio/decision/barrier", false);

        Assert.assertEquals(ReplyCode.KO_NODE_NOT_REPLY.getCode(), reply.getErrorCode());
    }

    @Test
    public void testManageBarrierResultsShouldBeKO() throws Exception {

        when(nodeReply.getErrorCode()).thenReturn(ReplyCode.KO_GENERAL_ERROR.getCode());
        when(nodeReply.getDescription()).thenReturn(ReplyCode.KO_GENERAL_ERROR.getMessage());

        ActionCallbackDto reply = WhiteboxImpl.invokeMethod(clusterSyncManager, "manageBarrierResults", message,
                nodeReply, "/stratio/decision/barrier", true);

        Assert.assertEquals(ReplyCode.KO_GENERAL_ERROR.getCode(), reply.getErrorCode()) ;
    }

    @Test
    public void testManageBarrierResultsShouldBeOK() throws Exception {

        when(nodeReply.getErrorCode()).thenReturn(ReplyCode.KO_GENERAL_ERROR.getCode());
        when(nodeReply.getDescription()).thenReturn(ReplyCode.KO_GENERAL_ERROR.getMessage());

        ActionCallbackDto reply = WhiteboxImpl.invokeMethod(clusterSyncManager, "manageBarrierResults", message,
                nodeReply, "/stratio/decision/barrier", true);

        Assert.assertEquals(ReplyCode.KO_GENERAL_ERROR.getCode(), reply.getErrorCode());
    }

//    @Test
//    public void testManageBarrierResults() throws Exception {
//
//        ActionCallbackDto parsedResponse = mock(ActionCallbackDto.class);
//        when(parsedResponse.getErrorCode()).thenReturn(ReplyCode.KO_GENERAL_ERROR.getCode());
//        when(parsedResponse.getDescription()).thenReturn(ReplyCode.KO_GENERAL_ERROR.getMessage());
//
//        final Gson mockedGson = PowerMockito.mock(Gson.class);
//        when(mockedGson.fromJson(anyString(), any())).thenReturn(ReplyCode.KO_GENERAL_ERROR.getCode());
//        PowerMockito.whenNew(Gson.class).withNoArguments().thenReturn(mockedGson);
//
//        ActionCallbackDto reply = WhiteboxImpl.invokeMethod(clusterSyncManager, "manageBarrierResults", message,
//                nodeReply, "/stratio/decision/barrier", true);
//
//        Assert.assertEquals(ReplyCode.KO_GENERAL_ERROR.getCode(), reply.getErrorCode());
//    }



}
