package com.stratio.streaming.functions.validator;

import com.stratio.streaming.commons.constants.ReplyCode;
import com.stratio.streaming.commons.constants.StreamAction;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.commons.messages.StreamQuery;
import com.stratio.streaming.configuration.ServiceConfiguration;
import com.stratio.streaming.configuration.StreamingSiddhiConfiguration;
import com.stratio.streaming.dao.StreamStatusDao;
import com.stratio.streaming.exception.RequestValidationException;
import com.stratio.streaming.service.CallbackService;
import com.stratio.streaming.service.StreamOperationService;
import com.stratio.streaming.service.StreamsHelper;
import org.apache.commons.collections.set.ListOrderedSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.SiddhiManager;

import java.util.Set;

import static org.junit.Assert.*;

/**
 * Created by aitor on 9/24/15.
 */
public class ValidationsTest {

    protected SiddhiManager siddhiManager;

    protected StreamStatusDao streamStatusDao;

    protected CallbackService callbackService;

    protected StreamOperationService streamOperationsService;

    @Before
    public void setUp() throws Exception {
        siddhiManager= new StreamingSiddhiConfiguration().siddhiManager();
        streamStatusDao= new StreamStatusDao();
        ServiceConfiguration serviceConfiguration= new ServiceConfiguration();
        callbackService= serviceConfiguration.callbackService();

        streamOperationsService= new StreamOperationService(siddhiManager, streamStatusDao, callbackService);
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testActionEnabledValidation()   throws Exception {
        ActionEnabledValidation validation= new ActionEnabledValidation(streamOperationsService, StreamAction.LISTEN, 1);
        StratioStreamingMessage message= StreamsHelper.getSampleMessage();
        validation.validate(message);

        streamOperationsService.createStream(StreamsHelper.STREAM_NAME, StreamsHelper.COLUMNS);
        streamOperationsService.enableAction(message.getStreamName(), StreamAction.LISTEN);
        validation= new ActionEnabledValidation(streamOperationsService, StreamAction.LISTEN, 1);

        int code= -1;
        try {
            validation.validate(message);
        } catch (RequestValidationException ex) {
            code= ex.getCode();
        }
        assertEquals(1, code);
    }

    @Test
    public void testStreamNameNotEmptyValidation()   throws Exception {
        StreamNameNotEmptyValidation validation= new StreamNameNotEmptyValidation();
        StratioStreamingMessage message= StreamsHelper.getSampleMessage();
        validation.validate(message);

        int code= -1;
        try {
            message.setStreamName("");
            validation.validate(message);
        } catch (RequestValidationException ex) {
            code= ex.getCode();
        }
        assertEquals(ReplyCode.KO_STREAM_OPERATION_NOT_ALLOWED.getCode(), (Integer) code);
    }

    @Test
    public void testQueryExistsValidation()   throws Exception {
        QueryExistsValidation validation= new QueryExistsValidation(streamOperationsService);
        StratioStreamingMessage message= StreamsHelper.getSampleMessage();
        streamOperationsService.createStream(StreamsHelper.STREAM_NAME, StreamsHelper.COLUMNS);
        String queryId= streamOperationsService.addQuery(StreamsHelper.STREAM_NAME, StreamsHelper.QUERY);

        validation.validate(message);

        validation= new QueryExistsValidation(streamOperationsService);

        message.setRequest(StreamsHelper.QUERY);

        int code= -1;
        try {
            validation.validate(message);
        } catch (RequestValidationException ex) {
            code= ex.getCode();
        }
        assertEquals(ReplyCode.KO_QUERY_ALREADY_EXISTS.getCode(), (Integer) code);

    }
}