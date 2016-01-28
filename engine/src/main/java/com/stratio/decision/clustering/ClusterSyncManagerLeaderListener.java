package com.stratio.decision.clustering;

import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import com.stratio.decision.task.FailOverTask;

/**
 * Created by josepablofernandez on 11/01/16.
 */
public class ClusterSyncManagerLeaderListener implements LeaderLatchListener {

    private static Logger logger = LoggerFactory.getLogger(ClusterSyncManagerLeaderListener.class);

    private FailOverTask failOverTask;
    private ClusterSyncManager clusterSyncManagerInstance;


    public ClusterSyncManagerLeaderListener(FailOverTask failOverTask, ClusterSyncManager clusterSyncManagerInstance){

        this.clusterSyncManagerInstance = clusterSyncManagerInstance;
        this.failOverTask = failOverTask;
    }

    @Override public void isLeader() {

        this.initializeFailOverTask();
        this.initializeNodeStatusPathCache();

    }

    @Override public void notLeader() {

    }

    private void initializeFailOverTask(){

        if (failOverTask!=null) {
            ThreadPoolTaskScheduler taskScheduler = new ThreadPoolTaskScheduler();
            taskScheduler.initialize();

            taskScheduler.scheduleAtFixedRate(failOverTask, 60000);
        }

    }

    private void initializeNodeStatusPathCache() {

        if (clusterSyncManagerInstance != null){
            try {
                clusterSyncManagerInstance.initializedNodeStatusPathCache();
            } catch (Exception e) {
                logger.error("Error initializing PathCache for Node Status Path: {}", e.getMessage());
            }
        }

    }


}
