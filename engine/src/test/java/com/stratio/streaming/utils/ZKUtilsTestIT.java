package com.stratio.streaming.utils;

import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.streaming.commons.constants.STREAM_OPERATIONS;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.service.StreamsHelper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Created by aitor on 9/30/15.
 */
public class ZKUtilsTestIT {

    private static Config conf;
    private static ZKUtils zkUtils;

    @BeforeClass
    public static void setUp() throws Exception {
        conf= ConfigFactory.load();
        zkUtils= ZKUtils.getZKUtils(conf.getString("zookeeper.hosts"));
        //LOGGER.debug("Zookeeper hosts: " + conf.getString("zookeeper.hosts"));
        //LOGGER.debug("Kafka hosts: " + conf.getString("kafka.hosts"));
    }

    @AfterClass
    public static void tearDown() throws Exception {
        ZKUtils.shutdownZKUtils();
    }

    @Test
    public void testCreateEphemeralZNode() throws Exception {
        String path= "/stratio/streaming/test-data";
        byte[] bytes= "test".getBytes();
        Exception ex= null;
        try {
            zkUtils.createEphemeralZNode(path, bytes);
        } catch (Exception e)   { ex= e; }

        assertNull("Not exception expected", ex);
    }

    @Test
    public void testCreateZNode() throws Exception {
        String path= "/stratio/streaming/myTestNode";
        byte[] bytes= "test".getBytes();

        Exception ex= null;
        try {
            zkUtils.createZNode(path, bytes);
        } catch (Exception e)   { ex= e; }

        assertNull("Not exception expected", ex);
        assertTrue("The Node has not been created", zkUtils.existZNode(path));
    }


    @Test
    public void testCreateZNodeJsonReply() throws Exception {
        String basePath= "/stratio/streaming/";
        String reply= "myValue";
        String operation= STREAM_OPERATIONS.ACTION.LISTEN;
        String requestId= "requestId";
        String path= basePath + operation.toLowerCase() + "/" + requestId;

        StratioStreamingMessage message= StreamsHelper.getSampleMessage();
        message.setOperation(operation);
        message.setRequest_id(requestId);

        Exception ex= null;
        try {
            zkUtils.createZNodeJsonReply(message, reply);
        } catch (Exception e)   { ex= e; }

        assertNull("Not exception expected", ex);
        assertTrue("The Node has not been created", zkUtils.existZNode(path));
        String result= new String(zkUtils.getZNode(path));//Base64.encodeBase64String(zkUtils.getZNode(path));
        assertEquals("Unexpected content from node", "\"" + reply + "\"", result);
    }


}