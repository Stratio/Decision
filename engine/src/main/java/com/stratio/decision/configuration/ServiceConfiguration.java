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

import com.stratio.decision.commons.messages.StratioStreamingMessage;
import com.stratio.decision.dao.StreamStatusDao;
import com.stratio.decision.dao.StreamingFailoverDao;
import com.stratio.decision.drools.DroolsConnectionContainer;
import com.stratio.decision.factory.GsonFactory;
import com.stratio.decision.serializer.Serializer;
import com.stratio.decision.serializer.impl.JavaToSiddhiSerializer;
import com.stratio.decision.serializer.impl.KafkaToJavaSerializer;
import com.stratio.decision.service.*;
import kafka.javaapi.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;

@Configuration
@Import({ DaoConfiguration.class, StreamingSiddhiConfiguration.class, DroolsConfiguration.class})
public class ServiceConfiguration {

    @Autowired
    private SiddhiManager siddhiManager;

    @Autowired
    private StreamStatusDao streamStatusDao;

    @Autowired
    @Lazy
    private StreamingFailoverDao streamingFailoverDao;

    @Autowired
    private Producer<String, String> producer;

    @Autowired
    private DroolsConnectionContainer droolsConnectionContainer;

    @Bean
    public StreamOperationService streamOperationService() {
        //return new StreamOperationService(siddhiManager, streamStatusDao, callbackService());
        return new StreamOperationService(siddhiManager, streamStatusDao, callbackService(), droolsConnectionContainer);
    }

    @Bean
    public StreamMetadataService streamMetadataService() {
        return new StreamMetadataService(siddhiManager);
    }

    @Bean
    public CallbackService callbackService() {
        return new CallbackService(producer, kafkaToJavaSerializer(), javaToSiddhiSerializer());
    }

    @Bean
    public Serializer<StratioStreamingMessage, Event> javaToSiddhiSerializer() {
        return new JavaToSiddhiSerializer(streamMetadataService());
    }

    @Bean
    public Serializer<String, StratioStreamingMessage> kafkaToJavaSerializer() {
        return new KafkaToJavaSerializer(GsonFactory.getInstance());
    }

    @Bean
    public StreamStatusMetricService streamStatusMetricService() {
        return new StreamStatusMetricService(streamStatusDao);
    }

    @Bean
    @Lazy
    public StreamingFailoverService streamingFailoverService() {
        return new StreamingFailoverService(streamStatusDao, streamMetadataService(), streamingFailoverDao);
    }


}
