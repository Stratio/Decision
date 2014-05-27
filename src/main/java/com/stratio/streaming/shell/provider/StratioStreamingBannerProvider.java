/*
 * Copyright 2011-2012 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.streaming.shell.provider;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.shell.plugin.support.DefaultBannerProvider;
import org.springframework.shell.support.util.OsUtils;
import org.springframework.stereotype.Component;

import com.stratio.streaming.api.IStratioStreamingAPI;

/**
 * @author Jarred Li
 * 
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class StratioStreamingBannerProvider extends DefaultBannerProvider {

    @Value("${kafka.host}")
    private String kafkaHost;

    @Value("${kafka.port}")
    private String kafkaPort;

    @Value("${zookeeper.host}")
    private String zookeeperHost;

    @Value("${zookeeper.port}")
    private String zookeeperPort;

    @Autowired
    private IStratioStreamingAPI stratioStreamingAPI;

    @Override
    public String getBanner() {
        return generateBanner();
    }

    @Override
    public String getVersion() {
        return "ALPHA";
    }

    @Override
    public String getWelcomeMessage() {
        return "Welcome to Stratio Streaming Shell";
    }

    @Override
    public String getProviderName() {
        return "Init Banner Provider";
    }

    private String generateBanner() {
        StringBuffer buf = new StringBuffer();
        buf.append("  _____ _             _   _         _____ _                            _             "
                + OsUtils.LINE_SEPARATOR);
        buf.append(" /  ___| |           | | (_)       /  ___| |                          (_)            "
                + OsUtils.LINE_SEPARATOR);
        buf.append(" \\ `--.| |_ _ __ __ _| |_ _  ___   \\ `--.| |_ _ __ ___  __ _ _ __ ___  _ _ __   __ _ "
                + OsUtils.LINE_SEPARATOR);
        buf.append("  `--. \\ __| '__/ _` | __| |/ _ \\   `--. \\ __| '__/ _ \\/ _` | '_ ` _ \\| | '_ \\ / _` |"
                + OsUtils.LINE_SEPARATOR);
        buf.append(" /\\__/ / |_| | | (_| | |_| | (_) | /\\__/ / |_| | |  __/ (_| | | | | | | | | | | (_| |"
                + OsUtils.LINE_SEPARATOR);
        buf.append(" \\____/ \\__|_|  \\__,_|\\__|_|\\___/  \\____/ \\__|_|  \\___|\\__,_|_| |_| |_|_|_| |_|\\__, |"
                + OsUtils.LINE_SEPARATOR);
        buf.append("                                                                                __/ |"
                + OsUtils.LINE_SEPARATOR);
        buf.append("                                                                               |___/ "
                + OsUtils.LINE_SEPARATOR);

        buf.append("Version:" + this.getVersion() + OsUtils.LINE_SEPARATOR);
        buf.append(OsUtils.LINE_SEPARATOR);
        buf.append("Connection urls: " + OsUtils.LINE_SEPARATOR);
        buf.append("    - Kafka: " + kafkaHost + ":" + kafkaPort + OsUtils.LINE_SEPARATOR);
        buf.append("    - Zookeeper: " + zookeeperHost + ":" + zookeeperPort + OsUtils.LINE_SEPARATOR);

        return buf.toString();
    }
}