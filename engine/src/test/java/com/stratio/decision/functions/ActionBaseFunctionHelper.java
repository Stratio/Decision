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
package com.stratio.decision.functions;

import com.stratio.decision.clustering.ClusterSyncManager;
import com.stratio.decision.commons.messages.StratioStreamingMessage;
import com.stratio.decision.configuration.ConfigurationContext;
import com.stratio.decision.configuration.SchredulerConfiguration;
import com.stratio.decision.configuration.ServiceConfiguration;
import com.stratio.decision.configuration.StreamingSiddhiConfiguration;
import com.stratio.decision.dao.StreamStatusDao;
import com.stratio.decision.functions.validator.RequestValidation;
import com.stratio.decision.functions.validator.StreamNameNotEmptyValidation;
import com.stratio.decision.service.CallbackService;
import com.stratio.decision.service.StreamOperationService;
import com.stratio.decision.service.StreamingFailoverService;
import com.stratio.decision.service.StreamsHelper;
import com.stratio.decision.task.FailOverTask;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.collections.set.ListOrderedSet;

import org.wso2.siddhi.core.SiddhiManager;

import java.util.List;
import java.util.Properties;
import java.util.Set;

import kafka.producer.ProducerConfig;
import kafka.javaapi.producer.Producer;

/**
 * Created by aitor on 9/23/15.
 */
public abstract class ActionBaseFunctionHelper {

    protected SiddhiManager siddhiManager;

    protected StreamStatusDao streamStatusDao;

    protected CallbackService callbackService;

    protected StreamOperationService streamOperationsService;

    protected Producer producer;

    protected Set<RequestValidation> validators;

    protected StratioStreamingMessage message;

    protected static Config conf;

    protected static String ZOO_HOST;

    protected static String MONGO_HOST;

    protected void initialize()  {
        conf= ConfigFactory.load();

        ZOO_HOST= getHostsStringFromList(conf.getStringList("zookeeper.hosts"));
        MONGO_HOST= getHostsStringFromList(conf.getStringList("mongo.hosts"));

        siddhiManager= new StreamingSiddhiConfiguration().siddhiManager();
        streamStatusDao= new StreamStatusDao();
        ServiceConfiguration serviceConfiguration= new ServiceConfiguration();
        callbackService= serviceConfiguration.callbackService();

        streamOperationsService= new StreamOperationService(siddhiManager, streamStatusDao, callbackService);

        streamOperationsService.createStream(StreamsHelper.STREAM_NAME, StreamsHelper.COLUMNS);
        String queryId= streamOperationsService.addQuery(StreamsHelper.STREAM_NAME, StreamsHelper.QUERY);
        message= StreamsHelper.getSampleMessage();
        message.setRequest(StreamsHelper.QUERY);

        validators= new ListOrderedSet();
        StreamNameNotEmptyValidation validation= new StreamNameNotEmptyValidation();
        validators.add(validation);

        Properties properties = new Properties();
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("metadata.broker.list",  conf.getStringList("kafka.hosts").get(0));
        properties.put("producer.type", "async");

        producer = new kafka.javaapi.producer.Producer<String, String>(new ProducerConfig(properties));

        ConfigurationContext configurationContext = new ConfigurationContext();
        try {
            ClusterSyncManager.getClusterSyncManager(configurationContext, null);
        } catch (Exception e) {
            e.printStackTrace();
        }
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