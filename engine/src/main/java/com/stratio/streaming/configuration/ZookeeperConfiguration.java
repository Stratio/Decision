package com.stratio.streaming.configuration;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import com.stratio.streaming.commons.constants.STREAMING;
import com.stratio.streaming.utils.ZKUtils;

@Configuration
// TODO refactor
public class ZookeeperConfiguration {

    @Autowired
    private ConfigurationContext configurationContext;

    @PostConstruct
    public void startUp() throws Exception {
        ZKUtils.getZKUtils(configurationContext.getZookeeperHostsQuorum()).createEphemeralZNode(
                STREAMING.ZK_BASE_PATH + "/" + "engine", String.valueOf(System.currentTimeMillis()).getBytes());
    }

}
