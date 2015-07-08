//package com.stratio.streaming.utils.hazelcast;
//
//import com.hazelcast.config.Config;
//import com.hazelcast.core.Hazelcast;
//import com.hazelcast.core.HazelcastInstance;
//import com.stratio.streaming.configuration.ConfigurationContext;
//import com.stratio.streaming.configuration.ServiceConfiguration;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import org.springframework.context.annotation.Import;
//
//import java.io.File;
//import java.io.IOException;
//
///**
// * Hazelcast instance factory.
// */
//@Import({ServiceConfiguration.class})
//public class HazelcastInstanceFactory {
//
//    private static Logger log = LoggerFactory.getLogger(HazelcastInstanceFactory.class);
//
//    private String hazelcastInstanceName = "streaming";
//    private String hazelcastConfigPath = "hazelcast.xml";
//
//    @Autowired
//    private ConfigurationContext configurationContext;
//
//    private HazelcastInstance hazelcastInstance;
//
//    @Bean(name = "hazelcastInstance", initMethod = "init", destroyMethod = "destroy")
//    public HazelcastInstance createHazelcastInstance() throws IOException{
//        return this.hazelcastInstance;
//    }
//
//    public void init() throws IOException {
//        log.debug("Creating Hazelcast instance");
//        this.hazelcastInstanceName = configurationContext.getHazelcastInstanceName();
//        this.hazelcastConfigPath = configurationContext.getHazelcastConfigPath();
//        Config config = new Config();
//        config.setInstanceName(hazelcastInstanceName);
//        File f = new File(getClass().getClassLoader().getResource(hazelcastConfigPath).getPath());
//        if (f.exists() && !f.isDirectory()) {
//            log.debug("Using Hazelcast with configuration [" + hazelcastConfigPath + "]");
//            config.setConfigurationFile(f);
//        } else {
//            throw new IOException("Hazelcast configuration [" + hazelcastConfigPath + "] not found");
//        }
//        this.hazelcastInstance = Hazelcast.getOrCreateHazelcastInstance(config);
//    }
//
//    public void destroy() {
//        log.debug("Stopping Hazelcast instance");
//        this.hazelcastInstance.getLifecycleService().shutdown();
//    }
//
//
//}