package com.stratio.streaming.functions;

import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.configuration.ServiceConfiguration;
import com.stratio.streaming.configuration.StreamingSiddhiConfiguration;
import com.stratio.streaming.dao.StreamStatusDao;
import com.stratio.streaming.functions.validator.RequestValidation;
import com.stratio.streaming.functions.validator.StreamNameNotEmptyValidation;
import com.stratio.streaming.service.CallbackService;
import com.stratio.streaming.service.StreamOperationService;
import com.stratio.streaming.service.StreamsHelper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.commons.collections.set.ListOrderedSet;
import org.junit.After;
import org.junit.Before;
import org.wso2.siddhi.core.SiddhiManager;

import java.util.Set;

import static org.junit.Assert.*;

/**
 * Created by aitor on 9/23/15.
 */
public abstract class ActionBaseFunctionHelper {

    protected SiddhiManager siddhiManager;

    protected StreamStatusDao streamStatusDao;

    protected CallbackService callbackService;

    protected StreamOperationService streamOperationsService;

    protected Set<RequestValidation> validators;

    protected StratioStreamingMessage message;

    protected static Config conf;

    protected static String ZOO_HOST;

    protected void initialize()  {
        conf= ConfigFactory.load();

        ZOO_HOST= conf.getString("zookeeper.hosts");

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
    }

}