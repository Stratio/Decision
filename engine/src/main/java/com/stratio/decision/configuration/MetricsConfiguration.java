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

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.ryantenney.metrics.spring.config.annotation.EnableMetrics;
import com.ryantenney.metrics.spring.config.annotation.MetricsConfigurerAdapter;
import com.stratio.decision.dao.StreamStatusDao;
import com.stratio.decision.metrics.SiddhiStreamReporter;
import com.stratio.decision.service.CallbackService;
import com.stratio.decision.service.StreamOperationServiceWithoutMetrics;
import com.stratio.decision.service.StreamStatusMetricService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.wso2.siddhi.core.SiddhiManager;

import java.util.concurrent.TimeUnit;

@Configuration
@Import({DaoConfiguration.class})
@EnableMetrics
public class MetricsConfiguration extends MetricsConfigurerAdapter {

    @Autowired
    private StreamStatusDao streamStatusDao;

    @Autowired
    private ConfigurationContext configurationContext;

    @Override
    public void configureReporters(MetricRegistry metricRegistry) {
        if (configurationContext.isPrintStreams()) {
            ConsoleReporter.forRegistry(metricRegistry).build().start(5, TimeUnit.SECONDS);
        }
        if (configurationContext.isStatsEnabled()) {
            SiddhiStreamReporter.forRegistry(metricRegistry, streamOperationServiceWithoutMetrics()).build().start(5, TimeUnit.SECONDS);
        }
        JmxReporter.forRegistry(metricRegistry).build().start();
    }

    @Bean
    public StreamStatusMetricService streamStatusMetricService() {
        return new StreamStatusMetricService(streamStatusDao);
    }

    @Autowired
    private SiddhiManager siddhiManager;

    @Autowired
    private CallbackService callbackService;

    @Bean
    public StreamOperationServiceWithoutMetrics streamOperationServiceWithoutMetrics() {
        return new StreamOperationServiceWithoutMetrics(siddhiManager, streamStatusDao, callbackService);
    }
}
