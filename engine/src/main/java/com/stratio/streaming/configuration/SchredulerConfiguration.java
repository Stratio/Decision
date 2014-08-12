package com.stratio.streaming.configuration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import com.stratio.streaming.service.StreamingFailoverService;
import com.stratio.streaming.task.FailOverTask;

@Configuration
@Import(ServiceConfiguration.class)
public class SchredulerConfiguration {

    @Autowired
    private ConfigurationContext configurationContext;

    @Autowired
    private StreamingFailoverService streamingFailoverService;

    @Bean
    public TaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler taskScheduler = new ThreadPoolTaskScheduler();
        taskScheduler.initialize();
        if (configurationContext.isFailOverEnabled()) {
            taskScheduler.scheduleAtFixedRate(failOverTask(), configurationContext.getFailOverPeriod());
        }
        return taskScheduler;
    }

    public FailOverTask failOverTask() {
        return new FailOverTask(streamingFailoverService);
    }
}
