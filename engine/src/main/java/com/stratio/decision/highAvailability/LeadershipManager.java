/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.decision.highAvailability;

import com.stratio.decision.commons.constants.STREAMING;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.kohsuke.randname.RandomNameGenerator;

import java.io.EOFException;
import java.io.IOException;
import java.util.Random;

public class LeadershipManager {

    private static LeadershipManager self;

    private CuratorFramework client;
    private String latchpath;
    private String id;
    private LeaderLatch leaderLatch;

    public LeadershipManager(String connString, String latchpath, String id) {
        this.client = CuratorFrameworkFactory.newClient(connString, 10000, 8000, new ExponentialBackoffRetry(1000, Integer.MAX_VALUE));
        this.latchpath = latchpath;
        this.id = id;
    }


//    public static LeadershipManager getLeadershipManager(String zookeeperCluster) throws Exception {
//        if (self == null) {
//            Random r = new Random();
//            RandomNameGenerator rnd = new RandomNameGenerator(r.nextInt());
//
//            self = new LeadershipManager(zookeeperCluster, STREAMING.ZK_HIGH_AVAILABILITY_PATH, rnd.next());
//        }
//        return self;
//    }

    public static LeadershipManager getLeadershipManager(String zookeeperCluster, String groupId) throws Exception {
        if (self == null) {
            Random r = new Random();
            RandomNameGenerator rnd = new RandomNameGenerator(r.nextInt());

            String zkPath = STREAMING.ZK_BASE_PATH.concat("/").concat(groupId).concat(STREAMING.ZK_HIGH_AVAILABILITY_NODE);
            self = new LeadershipManager(zookeeperCluster, zkPath, rnd.next());
        }
        return self;
    }

    public static LeadershipManager getNode() throws Exception {
        return self;
    }

    public void start() throws Exception {
        client.start();
        client.getZookeeperClient().blockUntilConnectedOrTimedOut();
        leaderLatch = new LeaderLatch(client, latchpath, id);
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

}
