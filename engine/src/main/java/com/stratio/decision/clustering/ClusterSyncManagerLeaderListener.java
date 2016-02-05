package com.stratio.decision.clustering;

import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import com.stratio.decision.commons.constants.STREAMING;
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

    private String initializeNodeStatusPathCache() {

        if (clusterSyncManagerInstance != null){
            try {

                PathChildrenCache cache = new PathChildrenCache(clusterSyncManagerInstance.getClient(), STREAMING
                        .ZK_EPHEMERAL_GROUPS_STATUS_BASE_PATH, true);

                cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
                ClusterPathChildrenCacheListener listener = new ClusterPathChildrenCacheListener(this
                        .clusterSyncManagerInstance);
                cache.getListenable().addListener(listener);

                return clusterSyncManagerInstance.initializedGroupStatus();

            } catch (Exception e) {
                logger.error("Error initializing PathCache for Node Status Path: {}", e.getMessage());
            }
        }

        return null;

    }


}
