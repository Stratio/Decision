package com.stratio.decision.clustering;

import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import com.stratio.decision.task.FailOverTask;

/**
 * Created by josepablofernandez on 11/01/16.
 */
public class ClusterSyncManagerLeaderListener implements LeaderLatchListener {


    private FailOverTask failOverTask;


    public ClusterSyncManagerLeaderListener(FailOverTask failOverTask) {

        this.failOverTask = failOverTask;
    }

    @Override public void isLeader() {

        this.initializeFailOverTask();

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


}
