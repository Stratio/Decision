package com.stratio.decision.configuration;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * Created by ajnavarro on 27/11/14.
 */
@Configuration
@Import({DaoConfiguration.class, StreamingSiddhiConfiguration.class})
public class StreamOperationServiceWithoutMetricsConfiguration {

}
