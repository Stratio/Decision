package com.stratio.streaming.configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiConfiguration;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.stratio.streaming.extensions.DistinctWindowExtension;
import com.stratio.streaming.streams.Casandra2PersistenceStore;
import com.stratio.streaming.streams.StreamPersistence;

@Configuration
// TODO refactor
public class StreamingSiddhiConfiguration {

    public static final String QUERY_PLAN_IDENTIFIER = "StratioStreamingCEP-Cluster";

    @Autowired
    private ConfigurationContext configurationContext;

    @Bean(destroyMethod = "shutdown")
    public SiddhiManager siddhiManager() {
        SiddhiConfiguration conf = new SiddhiConfiguration();
        conf.setInstanceIdentifier("StratioStreamingCEP-Instance-" + UUID.randomUUID().toString());
        conf.setQueryPlanIdentifier(QUERY_PLAN_IDENTIFIER);
        conf.setDistributedProcessing(false);

        @SuppressWarnings("rawtypes")
        List<Class> extensions = new ArrayList<>();
        extensions.add(DistinctWindowExtension.class);
        conf.setSiddhiExtensions(extensions);

        // Create Siddhi Manager
        SiddhiManager siddhiManager = new SiddhiManager(conf);

        Config config = new Config();
        config.setInstanceName("stratio-streaming-hazelcast-instance");
        NetworkConfig network = config.getNetworkConfig();
        JoinConfig join = network.getJoin();
        join.getMulticastConfig().setEnabled(false);

        siddhiManager.getSiddhiContext().setHazelcastInstance(HazelcastInstanceFactory.newHazelcastInstance(config));

        if (configurationContext.isFailOverEnabled()) {

            siddhiManager.setPersistStore(new Casandra2PersistenceStore(configurationContext.getCassandraHostsQuorum(),
                    "", ""));

            StreamPersistence.restoreLastRevision(siddhiManager);
        }

        return siddhiManager;
    }

}
