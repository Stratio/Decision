package com.stratio.streaming.configuration;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.ryantenney.metrics.spring.config.annotation.EnableMetrics;
import com.ryantenney.metrics.spring.config.annotation.MetricsConfigurerAdapter;
import com.stratio.streaming.dao.StreamStatusDao;
import com.stratio.streaming.service.StreamStatusMetricService;

@Configuration
@Import(DaoConfiguration.class)
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
        JmxReporter.forRegistry(metricRegistry).build().start();
    }

    @Bean
    public StreamStatusMetricService streamStatusMetricService() {
        return new StreamStatusMetricService(streamStatusDao);
    }

}
