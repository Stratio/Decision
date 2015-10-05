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
package com.stratio.decision.configuration;

import com.stratio.decision.service.StreamingFailoverService;
import com.stratio.decision.task.FailOverTask;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

@Configuration
@Import(ServiceConfiguration.class)
public class SchredulerConfiguration {

    @Autowired
    private ConfigurationContext configurationContext;

    @Autowired
    private StreamingFailoverService streamingFailoverService;

    @Bean
    public TaskScheduler taskScheduler() throws Exception {
        ThreadPoolTaskScheduler taskScheduler = new ThreadPoolTaskScheduler();
        taskScheduler.initialize();
        if (configurationContext.isFailOverEnabled()) {
            taskScheduler.scheduleAtFixedRate(failOverTask(), configurationContext.getFailOverPeriod());
        }
        return taskScheduler;
    }

    public FailOverTask failOverTask() throws Exception {
        return new FailOverTask(streamingFailoverService);
    }
}
