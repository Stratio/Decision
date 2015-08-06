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
package com.stratio.streaming;

import java.io.IOException;
import java.util.Map;

import com.stratio.streaming.configuration.FirstConfiguration;
import com.stratio.streaming.highAvailability.LeadershipManager;
import com.stratio.streaming.configuration.BaseConfiguration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import com.stratio.streaming.configuration.BaseConfiguration;

public class StreamingEngine {

    private static Logger log = LoggerFactory.getLogger(StreamingEngine.class);

    public static void main(String[] args) throws IOException {
        try (AnnotationConfigApplicationContext annotationConfigApplicationContextFirst = new AnnotationConfigApplicationContext(FirstConfiguration.class)) {
            LeadershipManager node = LeadershipManager.getNode();

            node.start();

            node.waitForLeadership();
            if (node.isLeader()) {
                System.out.println("I'm the leader");
                try (AnnotationConfigApplicationContext annotationConfigApplicationContext = new AnnotationConfigApplicationContext(
                        BaseConfiguration.class)) {
                    annotationConfigApplicationContext.registerShutdownHook();

                    Map<String, JavaStreamingContext> contexts = annotationConfigApplicationContext
                            .getBeansOfType(JavaStreamingContext.class);

                    for (JavaStreamingContext context : contexts.values()) {
                        context.start();
                        log.info("Started context {}", context.sparkContext().appName());
                    }
                    contexts.get("actionContext").awaitTermination();
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
