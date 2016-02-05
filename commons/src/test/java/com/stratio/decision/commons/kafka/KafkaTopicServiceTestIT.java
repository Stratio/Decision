package com.stratio.decision.commons.kafka;

import static org.junit.Assert.assertNull;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.stratio.decision.commons.kafka.service.KafkaTopicService;

/**
 * Created by eruiz on 5/10/15.
 */
public class KafkaTopicServiceTestIT {
    private KafkaTopicService func;
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTopicServiceTestIT.class);

    protected static Config conf;
    protected static String ZOO_HOST;


    @Before
    public void setUp() throws Exception {
        conf= ConfigFactory.load();
        ZOO_HOST= getHostsStringFromList(conf.getStringList("zookeeper.hosts"));

        LOGGER.debug("Connecting to kafka using Zookeeper Host: " + ZOO_HOST);

        func = new KafkaTopicService(ZOO_HOST, "",
                6667, 10000, 10000);
    }

    @Test
    public void testCreateTopicKafka() throws Exception {

        Exception ex = null;
        try {

            func.deleteTopics();

            func.createOrUpdateTopic("firstTopic", 1, 1);
            func.createTopicIfNotExist("firstTopic", 1, 1);
            func.createTopicIfNotExist("secondTopic", 1, 1);

        } catch (Exception e) {
            ex = e;
            ex.printStackTrace();
        }

        assertNull("Expected null value", ex);

        try {
            func.deleteTopics();
            func.close();
        }catch(org.I0Itec.zkclient.exception.ZkException e){
            ;
        }
    }

    @Test
    public void testCloseKafka() throws Exception {

        Exception ex = null;
        try {

            func.close();

        } catch (Exception e) {
            ex = e;
            ex.printStackTrace();
        }
        assertNull("Expected null value", ex);
    }


    protected String getHostsStringFromList(List<String> hosts)  {
        String hostsUrl= "";
        for (String host: hosts)    {
            hostsUrl += host + ",";
        }
        if (hostsUrl.length()>0)
            return hostsUrl.substring(0,hostsUrl.length()-1);
        else
            return "";
    }

}