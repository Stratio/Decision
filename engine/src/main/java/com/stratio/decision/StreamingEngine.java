/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.decision;

import com.stratio.decision.clustering.ClusterSyncManager;
import com.stratio.decision.configuration.BaseConfiguration;
import com.stratio.decision.configuration.ConfigurationContext;
import com.stratio.decision.configuration.FirstConfiguration;
import com.stratio.decision.highAvailability.LeadershipManager;
import com.stratio.decision.task.FailOverTask;
import com.stratio.decision.utils.ZKUtils;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.io.IOException;

public class StreamingEngine {

    private static Logger log = LoggerFactory.getLogger(StreamingEngine.class);

    public static void main(String[] args) throws IOException {
        try (AnnotationConfigApplicationContext annotationConfigApplicationContextFirst = new AnnotationConfigApplicationContext(FirstConfiguration.class)) {

            LeadershipManager node = LeadershipManager.getNode();

            node.start();

            node.waitForLeadership();
            if (node.isLeader()) {
                log.info("This is the Decision leader node.");
                try (AnnotationConfigApplicationContext annotationConfigApplicationContext = new AnnotationConfigApplicationContext(
                        BaseConfiguration.class)) {
                    ConfigurationContext configurationContext = annotationConfigApplicationContext.getBean("configurationContext", ConfigurationContext.class);

                    /**
                     * ClusterSyncManager Instance
                     */
                    FailOverTask failOverTask = null;
                    if (configurationContext.isFailOverEnabled()){
                        failOverTask = annotationConfigApplicationContext.getBean("failOverTask", FailOverTask
                                .class);
                    }

                    ClusterSyncManager
                            .getClusterSyncManager(configurationContext, failOverTask).start();

                    annotationConfigApplicationContext.registerShutdownHook();
                    JavaStreamingContext context = annotationConfigApplicationContext.getBean("streamingContext", JavaStreamingContext.class);
                    context.start();

                    ClusterSyncManager.getNode().initializedNodeStatus();

                    context.awaitTermination();

                } catch (Exception e) {
                    log.error("Fatal error", e);
                }
            }
        } catch (Exception e) {
            log.error("Fatal error", e);
        } finally {
            System.exit(0);
        }
    }
}