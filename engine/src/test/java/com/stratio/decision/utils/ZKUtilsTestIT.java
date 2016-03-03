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
package com.stratio.decision.utils;

import com.stratio.decision.commons.constants.STREAMING;
import com.stratio.decision.commons.constants.STREAM_OPERATIONS;
import com.stratio.decision.commons.messages.StratioStreamingMessage;
import com.stratio.decision.service.StreamsHelper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by aitor on 9/30/15.
 */
public class ZKUtilsTestIT {

    private static Logger LOGGER = LoggerFactory.getLogger(ZKUtils.class);

    private static Config conf;
    private static ZKUtils zkUtils;

    private static String ZOOKEEPER_HOSTS= "";

    @BeforeClass
    public static void setUp() throws Exception {
        conf= ConfigFactory.load();
        List<String> hosts= conf.getStringList("zookeeper.hosts");

        for (String host: hosts)    {
            ZOOKEEPER_HOSTS += host + ",";
        }
        ZOOKEEPER_HOSTS= ZOOKEEPER_HOSTS.substring(0,ZOOKEEPER_HOSTS.length()-1);

        zkUtils= ZKUtils.getZKUtils(ZOOKEEPER_HOSTS);
        LOGGER.debug("Using Zookeeper hosts: " + ZOOKEEPER_HOSTS);
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
            zkUtils.createZNode(path, bytes);
        } catch (Exception e)   { ex= e; }

        assertNull("Not exception expected", ex);
        assertTrue("The Node has not been created", zkUtils.existZNode(path));
    }


    @Test
    public void testCreateZNodeJsonReply() throws Exception {

        String reply= "myValue";
        String operation= STREAM_OPERATIONS.ACTION.LISTEN;
        String requestId= "requestId";
        String path= STREAMING.ZK_BASE_PATH + "/" + operation.toLowerCase() + "/" + requestId;

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