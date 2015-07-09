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
package com.stratio.streaming.configuration;

import com.stratio.streaming.utils.hazelcast.SharedSiddhiManager;
import com.stratio.streaming.utils.hazelcast.StreamingHazelcastInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.wso2.siddhi.core.SiddhiManager;

@Configuration
@Import({HazelcastConfiguration.class})
public class StreamingSiddhiConfiguration {

    @Autowired
    private ConfigurationContext configurationContext;

    @Autowired
    private StreamingHazelcastInstance streamingHazelcastInstance;

    @Bean(destroyMethod = "shutdown")
    public SiddhiManager siddhiManager() {
        return new SharedSiddhiManager(streamingHazelcastInstance);
    }

}
