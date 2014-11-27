package com.stratio.streaming.configuration;

import com.stratio.streaming.dao.StreamStatusDao;
import com.stratio.streaming.service.CallbackService;
import com.stratio.streaming.service.StreamOperationServiceWithoutMetrics;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.wso2.siddhi.core.SiddhiManager;

/**
 * Created by ajnavarro on 27/11/14.
 */
@Configuration
@Import({DaoConfiguration.class, StreamingSiddhiConfiguration.class})
public class StreamOperationServiceWithoutMetricsConfiguration {

}
