package com.stratio.decision.shell.configuration;

import com.stratio.decision.api.IStratioStreamingAPI;
import com.stratio.decision.shell.wrapper.StratioStreamingApiWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Created by ajnavarro on 24/11/14.
 */
@Configuration
public class StreamingApiWrapperConfiguration {
    @Autowired
    IStratioStreamingAPI stratioStreamingApi;

    @Bean
    public StratioStreamingApiWrapper stratioStreamingApiWrapper() {
        return new StratioStreamingApiWrapper(stratioStreamingApi);
    }
}
