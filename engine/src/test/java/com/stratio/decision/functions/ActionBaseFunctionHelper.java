package com.stratio.decision.functions;

import com.stratio.decision.commons.messages.StratioStreamingMessage;
import com.stratio.decision.configuration.ServiceConfiguration;
import com.stratio.decision.configuration.StreamingSiddhiConfiguration;
import com.stratio.decision.dao.StreamStatusDao;
import com.stratio.decision.functions.validator.RequestValidation;
import com.stratio.decision.functions.validator.StreamNameNotEmptyValidation;
import com.stratio.decision.service.CallbackService;
import com.stratio.decision.service.StreamOperationService;
import com.stratio.decision.service.StreamsHelper;
import org.apache.commons.collections.set.ListOrderedSet;
import org.wso2.siddhi.core.SiddhiManager;

import java.util.Set;

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

    protected static final String ZOO_HOST= "localhost";

    protected void initialize()  {
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