package com.stratio.streaming.utils.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Hazelcast instance.
 */
public class StreamingHazelcastInstance {

    private static Logger log = LoggerFactory.getLogger(StreamingHazelcastInstance.class);

    private HazelcastInstance hazelcastInstance;

    public StreamingHazelcastInstance(String hazelcastInstanceName, String hazelcastConfigPath) throws IOException {
        log.debug("Creating Hazelcast instance");
        Config config = new Config();
        config.setProperty("hazelcast.logging.type", "log4j");
        config.setProperty("log4j.configuration", "log4j.xml");
        config.setInstanceName(hazelcastInstanceName);
        File f = new File(getClass().getClassLoader().getResource(hazelcastConfigPath).getPath());
        if (f.exists() && !f.isDirectory()) {
            log.debug("Using Hazelcast with configuration [" + hazelcastConfigPath + "]");
            config.setConfigurationFile(f);
        } else {
            throw new IOException("Hazelcast configuration [" + hazelcastConfigPath + "] not found");
        }
        this.hazelcastInstance = Hazelcast.newHazelcastInstance(config);
        log.error("Number of hazelcast members in cluster");
    }

    public void destroy() {
        log.debug("Stopping Hazelcast instance");
        this.hazelcastInstance.getLifecycleService().shutdown();
    }

    public HazelcastInstance getHazelcastInstance() {
        return this.hazelcastInstance;
    }

}
