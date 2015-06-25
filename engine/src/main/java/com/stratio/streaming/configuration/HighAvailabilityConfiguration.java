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

import com.stratio.streaming.commons.constants.STREAMING;
import com.stratio.streaming.highAvailability.LeadershipManager;
import com.stratio.streaming.utils.ZKUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.util.Random;

@Configuration
public class HighAvailabilityConfiguration {

    private static final String PATH = "/latch";

    @Autowired
    private ConfigurationContext configurationContext;

    @PostConstruct
    public void startUp() throws Exception {
        LeadershipManager.getLeadershipManager(configurationContext.getZookeeperHostsQuorum());

    }

}
