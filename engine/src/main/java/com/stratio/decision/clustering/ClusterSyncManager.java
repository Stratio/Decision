package com.stratio.decision.clustering;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.kohsuke.randname.RandomNameGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.stratio.decision.commons.constants.ReplyCode;
import com.stratio.decision.commons.constants.STREAMING;
import com.stratio.decision.commons.dto.ActionCallbackDto;
import com.stratio.decision.commons.messages.StratioStreamingMessage;
import com.stratio.decision.configuration.ConfigurationContext;
import com.stratio.decision.task.FailOverTask;
import com.stratio.decision.utils.ZKUtils;

/**
 * Created by josepablofernandez on 11/01/16.
 */
public class ClusterSyncManager {

    private static Logger logger = LoggerFactory.getLogger(ClusterSyncManager.class);


    private static final String BARRIER_RELATIVE_PATH = "barrier";

    private static ClusterSyncManager self;

    private CuratorFramework client;
    private String latchpath;
    private String id;
    private LeaderLatch leaderLatch;

    private Boolean clusteringEnabled;
    private List<String> clusterNodes;
    private List<String> nodesToCheck;

    private Map<NODE_STATUS, List<String>> clusterNodesStatus;

    private String groupId;
    private FailOverTask failOverTask;
    private Boolean allAckEnabled;
    private int ackTimeout;

    private String zookeeperHost;

    public enum NODE_STATUS{

        STOPPED("stop"), INITIALIZED("initialized");

        private final String status;

        private NODE_STATUS(String status) {
            this.status = status;
        }

        public String getStatus() {
            return status;
        }
    }

    public ClusterSyncManager(String latchpath, String id, ConfigurationContext
            configurationContext, FailOverTask failOverTask) {

        this.zookeeperHost = configurationContext.getZookeeperHostsQuorum();
        this.client = CuratorFrameworkFactory
                .newClient(zookeeperHost, 10000, 8000, new ExponentialBackoffRetry(1000, Integer.MAX_VALUE));
        this.latchpath = latchpath;
        this.id = id;
        this.clusteringEnabled = configurationContext.isClusteringEnabled();
        this.clusterNodes = configurationContext.getClusterGroups();
        this.groupId = configurationContext.getGroupId();
        this.allAckEnabled = configurationContext.isAllAckEnabled();
        this.ackTimeout = configurationContext.getAckTimeout();
        this.failOverTask = failOverTask;

        if (clusteringEnabled && clusterNodes!= null){

            this.nodesToCheck = clusterNodes.stream().filter( name -> !name.equals(groupId)).collect(Collectors.toList());
        }
    }


    public static ClusterSyncManager getClusterSyncManager(ConfigurationContext configurationContext,
            FailOverTask failOverTask) throws  Exception {

        if (self == null) {
            Random r = new Random();
            RandomNameGenerator rnd = new RandomNameGenerator(r.nextInt());

            String zkPath =STREAMING.ZK_CLUSTER_MANAGER_PATH;
            self = new ClusterSyncManager(zkPath, rnd.next(), configurationContext, failOverTask);
        }
        return self;
    }

    public static ClusterSyncManager getNode() throws Exception {
        return self;
    }

    public void start() throws Exception {
        client.start();
        client.getZookeeperClient().blockUntilConnectedOrTimedOut();
        leaderLatch = new LeaderLatch(client, latchpath, id);

        ClusterSyncManagerLeaderListener listener = new ClusterSyncManagerLeaderListener(failOverTask, getNode());
        leaderLatch.addListener(listener);

        leaderLatch.start();
    }


    public void manageAckStreamingOperation(StratioStreamingMessage message, ActionCallbackDto reply) {

        try {

            // Single instance mode
            if (!clusteringEnabled){
                ZKUtils.getZKUtils(zookeeperHost).createZNodeJsonReply(message, reply);
            } else {
                // Sharding mode
                if (allAckEnabled) {

                    ZKUtils.getZKUtils(zookeeperHost).createTempZNodeJsonReply(message, reply, groupId);

                    String path = ZKUtils.getZKUtils(zookeeperHost).getTempZNodeJsonReplyPath(message);
                    String barrierPath = path.concat("/").concat(BARRIER_RELATIVE_PATH);

                    DistributedDoubleBarrier barrier = new DistributedDoubleBarrier(client, barrierPath,
                            clusterNodes.size());
                    boolean success = barrier.enter(ackTimeout, TimeUnit.MILLISECONDS);

                    if (isLeader()) {
                        manageBarrierResults(message, reply, path, success);
                    }

                } else {

                    if (isLeader()) {
                        ZKUtils.getZKUtils(zookeeperHost).createZNodeJsonReply(message, reply);
                    }
                }
            }

        }catch (Exception e){
            logger.error("Exception managing the ack of the group {} for the request {}: {}", groupId, message
                    .getRequest_id(), e
                    .getMessage());
        }

    }


    private void manageBarrierResults(StratioStreamingMessage message, ActionCallbackDto reply, String path, Boolean
            success) throws Exception {

        ActionCallbackDto clusterReply = reply;

         if (!success){
             logger.debug("Leaving ACK barrier for path: {} WITH NO SUCCESS", path);
             clusterReply= new ActionCallbackDto(ReplyCode.KO_NODE_NOT_REPLY.getCode(), ReplyCode.KO_NODE_NOT_REPLY
                      .getMessage());
         } else {

             logger.debug("Leaving ACK barrier for path: {} WITH SUCCESS", path);
             if (reply.getErrorCode() == ReplyCode.OK.getCode()) {

                 Gson gson = new Gson();
                 Boolean koResponse = false;

                 for (int i=0; i<nodesToCheck.size() && !koResponse; i++){

                     String nodePath = path.concat("/").concat(nodesToCheck.get(i));
                     String data = new String (client.getData().forPath(nodePath));

                     ActionCallbackDto parsedResponse = gson.fromJson(data, ActionCallbackDto.class);
                     if (parsedResponse.getErrorCode()!=  ReplyCode.OK.getCode()) {
                         clusterReply = parsedResponse;
                         koResponse = true;
                     }
                 }
             }

         }

        ZKUtils.getZKUtils(zookeeperHost).createZNodeJsonReply(message, clusterReply);
        client.delete().deletingChildrenIfNeeded().forPath(path);
    }


    public void initializedNodeStatus() throws Exception {

        if (!clusteringEnabled){
            ZKUtils.getZKUtils(zookeeperHost).createEphemeralZNode(STREAMING.ZK_EPHEMERAL_NODE_STATUS_PATH, STREAMING.ZK_EPHEMERAL_NODE_STATUS_INITIALIZED.getBytes());
        }
        else {
            ZKUtils.getZKUtils(zookeeperHost).createEphemeralZNode(STREAMING.ZK_EPHEMERAL_GROUPS_STATUS_BASE_PATH
                    .concat("/").concat(STREAMING.GROUPS_STATUS_BASE_PREFIX)
                    .concat(groupId), STREAMING.ZK_EPHEMERAL_NODE_STATUS_INITIALIZED.getBytes());
        }

    }

    /**
     * Called from ClusterSyncManagerLeaderListener
     * @throws Exception
     */
    public void initializedGroupStatusPathCache() throws Exception {

        if (isLeader() && clusteringEnabled) {

            PathChildrenCache cache = new PathChildrenCache(client, STREAMING.ZK_EPHEMERAL_GROUPS_STATUS_BASE_PATH,
                    true);
            cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
            addNodeStatusListener(cache);
            initClusterNodeStatus();
            updateNodeStatus();
        }
    }

    private void initClusterNodeStatus(){

        this.clusterNodesStatus = new HashMap<>();
        this.clusterNodesStatus.put(NODE_STATUS.INITIALIZED, new ArrayList<>());
        this.clusterNodesStatus.put(NODE_STATUS.STOPPED, new ArrayList<>());

        clusterNodes.forEach( node -> {

            String zkPath = STREAMING.ZK_EPHEMERAL_GROUPS_STATUS_BASE_PATH.concat("/").concat(STREAMING.GROUPS_STATUS_BASE_PREFIX)
                    .concat(node);

            Boolean existsNode = false;
            try {
                existsNode = ZKUtils.getZKUtils(zookeeperHost).existZNode(zkPath);
            }catch(Exception e){
                logger.error("Error checking ZK path {}: {}", zkPath, e.getMessage());
            }

            if (existsNode){
                clusterNodesStatus.get(NODE_STATUS.INITIALIZED).add(node);
            }else{
                clusterNodesStatus.get(NODE_STATUS.STOPPED).add(node);
            }

        });

    }

    private void addNodeStatusListener(PathChildrenCache cache){


        PathChildrenCacheListener listener = new PathChildrenCacheListener(){

                 @Override
                 public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
                 {
                     String node =  ZKPaths.getNodeFromPath(event.getData().getPath());
                     String nodeId = node.substring(node.indexOf("_")+1);

                         switch ( event.getType() ){

                                 case CHILD_ADDED:
                                         logger.info("STATUS - Group Initialized: {} ", nodeId);
                                        clusterNodesStatus.get(NODE_STATUS.INITIALIZED).add(nodeId);
                                        clusterNodesStatus.get(NODE_STATUS.STOPPED).remove(nodeId);
                                         break;
                                 case CHILD_UPDATED:
                                         logger.info("STATUS - Group changed: {}",  nodeId);
                                         break;
                                 case CHILD_REMOVED:
                                         logger.error("***** STATUS - Group {} are notified as DOWN *****", nodeId);
                                         clusterNodesStatus.get(NODE_STATUS.INITIALIZED).remove(nodeId);
                                         clusterNodesStatus.get(NODE_STATUS.STOPPED).add(nodeId);
                                         break;
                         }

                     updateNodeStatus();

                     }
             };
         cache.getListenable().addListener(listener);
     }

    private void updateNodeStatus() throws Exception {

       if (clusterNodesStatus.get(NODE_STATUS.STOPPED).size() == 0){

            logger.info("STATUS - All groups Initialized");
            ZKUtils.getZKUtils(zookeeperHost).createEphemeralZNode(STREAMING.ZK_EPHEMERAL_NODE_STATUS_PATH, STREAMING.ZK_EPHEMERAL_NODE_STATUS_INITIALIZED.getBytes());

        }else {

            logger.error("**** STATUS - Some groups are DOWN");
            clusterNodesStatus.get(NODE_STATUS.STOPPED).forEach( node -> logger.error("**** STATUS - groupId: {} is "
                    + "DOWN", node));

            ZKUtils.getZKUtils(zookeeperHost).createEphemeralZNode(STREAMING.ZK_EPHEMERAL_NODE_STATUS_PATH, STREAMING
                    .ZK_EPHEMERAL_NODE_STATUS_GROUPS_DOWN.getBytes());

        }

    }

    public boolean isLeader() {
        return leaderLatch.hasLeadership();
    }

    public Participant currentLeader() throws Exception {
        return leaderLatch.getLeader();
    }

    public void close() throws IOException {
        leaderLatch.close();
        client.close();
    }

    public void waitForLeadership() throws InterruptedException, EOFException {
        leaderLatch.await();
    }

}
