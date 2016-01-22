package com.stratio.decision.clustering;

import java.io.EOFException;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
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

    private static Logger logger = LoggerFactory.getLogger(ZKUtils.class);


    private static ClusterSyncManager self;

    private CuratorFramework client;
    private String latchpath;
    private String id;
    private LeaderLatch leaderLatch;

    private List<String> clusterNodes;
    private List<String> nodesToCheck;
    private String clusterId;
    private FailOverTask failOverTask;
    private Boolean allAckEnabled;

    private String zookeeperHost;

    public ClusterSyncManager(String latchpath, String id, ConfigurationContext
            configurationContext, FailOverTask failOverTask) {

        this.zookeeperHost = configurationContext.getZookeeperHostsQuorum();
        this.client = CuratorFrameworkFactory
                .newClient(zookeeperHost, 10000, 8000, new ExponentialBackoffRetry(1000, Integer.MAX_VALUE));
        this.latchpath = latchpath;
        this.id = id;
        this.clusterNodes = configurationContext.getClusterQuorum();
        this.clusterId = configurationContext.getClusterId();
        this.allAckEnabled = configurationContext.isAllAckEnabled();
        this.failOverTask = failOverTask;

        if (clusterNodes!= null && clusterNodes.size()>1){
            nodesToCheck = clusterNodes.stream().filter( name -> !name.equals(clusterId)).collect(Collectors.toList());
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

        ClusterSyncManagerLeaderListener listener = new ClusterSyncManagerLeaderListener(failOverTask);
        leaderLatch.addListener(listener);

        leaderLatch.start();
    }


    public void manageAckStreamingOperation(StratioStreamingMessage message, ActionCallbackDto reply) {

        try {

            if (isLeader()) {

                if (!allAckEnabled) {
                    ZKUtils.getZKUtils(zookeeperHost).createZNodeJsonReply(message, reply);
                }

                else{

                    String path =  ZKUtils.getZKUtils(zookeeperHost).getTempZNodeJsonReplyPath(message);
                    PathChildrenCache cache = new PathChildrenCache(client, path, true);
                    cache.start();

                    addListener(cache);

                }


            }
            else if (allAckEnabled){
                ZKUtils.getZKUtils(zookeeperHost).createTempZNodeJsonReply(message, reply, clusterId);
            }

        }catch (Exception e){
            ;
        }


    }

    private static void addListener(PathChildrenCache cache)
         {
               // a PathChildrenCacheListener is optional. Here, it's used just to log changes
                PathChildrenCacheListener listener = new PathChildrenCacheListener()
                 {
                         @Override
                         public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
                         {
                                 switch ( event.getType() )
                                 {
                                         case CHILD_ADDED:
                                          {
                                                 logger.error("Node added: " + ZKPaths
                                                         .getNodeFromPath(event.getData().getPath()));


                                                 break;
                                             }

                                         case CHILD_UPDATED:
                                             {
                                                 logger.error("Node changed: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
                                                 break;
                                             }

                                         case CHILD_REMOVED:
                                             {
                                                 logger.error("Node removed: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
                                                 break;

                                             }
                                     }
                             }
                     };
                 cache.getListenable().addListener(listener);
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
