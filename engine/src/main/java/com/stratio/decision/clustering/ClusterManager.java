package com.stratio.decision.clustering;

import java.io.EOFException;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.kohsuke.randname.RandomNameGenerator;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import com.stratio.decision.commons.constants.STREAMING;
import com.stratio.decision.task.FailOverTask;

/**
 * Created by josepablofernandez on 11/01/16.
 */
public class ClusterManager {

/*
    private static ClusterManager self;

    private CuratorFramework client;
    private String latchpath;
    private String id;
    private LeaderLatch leaderLatch;

    private List<String> clusterNodes;
    private FailOverTask failOverTask;

    public ClusterManager(String connString, String latchpath, String id, List<String> clusterNodes, FailOverTask failOverTask) {
        this.client = CuratorFrameworkFactory
                .newClient(connString, 10000, 8000, new ExponentialBackoffRetry(1000, Integer.MAX_VALUE));
        this.latchpath = latchpath;
        this.id = id;
        this.clusterNodes = clusterNodes;
        this.failOverTask = failOverTask;

    }

//    public void initializeFailOverTask() throws Exception{
//
//        boolean leader = getNode().isLeader();
//
//        if (failOverTask!=null && this.getNode().isLeader()) {
//            ThreadPoolTaskScheduler taskScheduler = new ThreadPoolTaskScheduler();
//            taskScheduler.initialize();
//
//            taskScheduler.scheduleAtFixedRate(failOverTask, 60000);
//        }
//
//    }



    public static ClusterManager getClusterManager(String zookeeperCluster, List<String> clusterNodes, FailOverTask failOverTask)
            throws
            Exception {

        if (self == null) {
            Random r = new Random();
            RandomNameGenerator rnd = new RandomNameGenerator(r.nextInt());

            String zkPath =STREAMING.ZK_CLUSTER_MANAGER_PATH;
            self = new ClusterManager(zookeeperCluster, zkPath, rnd.next(), clusterNodes, failOverTask);
        }
        return self;
    }

    public static ClusterManager getNode() throws Exception {
        return self;
    }

    public void start() throws Exception {
        client.start();
        client.getZookeeperClient().blockUntilConnectedOrTimedOut();
        leaderLatch = new LeaderLatch(client, latchpath, id);

        ClusterManagerLeaderListener listener = new ClusterManagerLeaderListener(failOverTask);
        leaderLatch.addListener(listener);

        leaderLatch.start();
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
    */
}
