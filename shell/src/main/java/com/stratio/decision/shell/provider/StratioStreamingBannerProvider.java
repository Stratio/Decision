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
package com.stratio.decision.shell.provider;

import com.stratio.decision.shell.Main;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.shell.plugin.support.DefaultBannerProvider;
import org.springframework.shell.support.util.FileUtils;
import org.springframework.shell.support.util.OsUtils;
import org.springframework.stereotype.Component;

/**
 * @author Jarred Li
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class StratioStreamingBannerProvider extends DefaultBannerProvider {

    @Value("${kafka.brokers}")
    private String kafkaBrokers;

    @Value("${zookeeper.quorum}")
    private String zkQuorum;

    @Override
    public String getBanner() {
        StringBuilder sb = new StringBuilder();
        sb.append(FileUtils.readBanner(Main.class, "/banner.txt"));
        sb.append(OsUtils.LINE_SEPARATOR);
        sb.append("Connection urls: " + OsUtils.LINE_SEPARATOR);
        sb.append("    - Kafka: " + kafkaBrokers);
        sb.append("    - Zookeeper: " + zkQuorum);
        sb.append(OsUtils.LINE_SEPARATOR);
        return sb.toString();
    }

    @Override
    public String getVersion() {
        return "1";
    }

    @Override
    public String getWelcomeMessage() {
        return "Type \"help\" to see all available commands.";
    }

    @Override
    public String getProviderName() {
        return "Stratio Streaming";
    }

}