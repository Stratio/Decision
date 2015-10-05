package com.stratio.decision.functions.validator;

import com.stratio.decision.commons.constants.ReplyCode;
import com.stratio.decision.commons.constants.STREAM_OPERATIONS;
import com.stratio.decision.commons.constants.StreamAction;
import com.stratio.decision.commons.messages.StratioStreamingMessage;
import com.stratio.decision.configuration.ServiceConfiguration;
import com.stratio.decision.configuration.StreamingSiddhiConfiguration;
import com.stratio.decision.dao.StreamStatusDao;
import com.stratio.decision.exception.RequestValidationException;
import com.stratio.decision.service.CallbackService;
import com.stratio.decision.service.StreamOperationService;
import com.stratio.decision.service.StreamsHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.SiddhiManager;

import static org.junit.Assert.assertEquals;

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
        assertEquals("Expected exception doesn't occurs", 1, code);
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
        assertEquals("Expected exception doesn't occurs",
                ReplyCode.KO_STREAM_OPERATION_NOT_ALLOWED.getCode(), (Integer) code);
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
        assertEquals("Expected exception doesn't occurs",
                ReplyCode.KO_QUERY_ALREADY_EXISTS.getCode(), (Integer) code);

    }

    @Test
    public void testQueryNotExistsValidation()   throws Exception {
        QueryNotExistsValidation validation= new QueryNotExistsValidation(streamOperationsService);
        StratioStreamingMessage message= StreamsHelper.getSampleMessage();
        message.setRequest(StreamsHelper.QUERY);

        streamOperationsService.createStream(StreamsHelper.STREAM_NAME, StreamsHelper.COLUMNS);
        String queryId= streamOperationsService.addQuery(StreamsHelper.STREAM_NAME, StreamsHelper.QUERY);

        int code= -1;
        try {
            validation.validate(message);
        } catch (RequestValidationException ex) {
            code= ex.getCode();
        }
        assertEquals("Expected exception doesn't occurs",
                ReplyCode.KO_QUERY_DOES_NOT_EXIST.getCode(), (Integer) code);


        //validation.validate(message);

    }


    @Test
    public void testQueryStreamAllowedValidation()   throws Exception {
        StreamAllowedValidation validation= new StreamAllowedValidation();
        StratioStreamingMessage message= StreamsHelper.getSampleMessage();

        message.setOperation(STREAM_OPERATIONS.ACTION.LISTEN);
        validation.validate(message);

        message.setOperation("Not Existing operation");

        int code= -1;
        try {
            validation.validate(message);
        } catch (RequestValidationException ex) {
            code= ex.getCode();
        }
        assertEquals("Expected exception doesn't occurs",
                ReplyCode.KO_STREAM_OPERATION_NOT_ALLOWED.getCode(), (Integer) code);

    }


    @Test
    public void testStreamNameByRegularExpressionValidation()   throws Exception {
        //String pattern= "^\\w";
        String pattern= "(test)(\\w+)";
        StreamNameByRegularExpressionValidator validation= new StreamNameByRegularExpressionValidator(pattern, StreamAction.LISTEN);
        StratioStreamingMessage message= StreamsHelper.getSampleMessage();
        validation.validate(message);

        validation= new StreamNameByRegularExpressionValidator("BAD PATTERN!!", StreamAction.LISTEN);

        int code= -1;
        try {
            validation.validate(message);
        } catch (RequestValidationException ex) {
            code= ex.getCode();
        }
        assertEquals("Expected exception doesn't occurs",
                ReplyCode.KO_STREAM_OPERATION_NOT_ALLOWED.getCode(), (Integer) code);

    }

    @Test
    public void testStreamExistsValidation()   throws Exception {
        streamOperationsService.createStream(StreamsHelper.STREAM_NAME, StreamsHelper.COLUMNS);
        String queryId= streamOperationsService.addQuery(StreamsHelper.STREAM_NAME, StreamsHelper.QUERY);

        StreamExistsValidation validation= new StreamExistsValidation(streamOperationsService);
        StratioStreamingMessage message= StreamsHelper.getSampleMessage();

        int code= -1;
        try {
            validation.validate(message);
        } catch (RequestValidationException ex) {
            code= ex.getCode();
        }
        assertEquals("Expected exception doesn't occurs",
                ReplyCode.KO_STREAM_ALREADY_EXISTS.getCode(), (Integer) code);

    }

    @Test
    public void testNotStreamExistsValidation()   throws Exception {

        StreamNotExistsValidation validation= new StreamNotExistsValidation(streamOperationsService);
        StratioStreamingMessage message= StreamsHelper.getSampleMessage();

        int code= -1;
        try {
            validation.validate(message);
        } catch (RequestValidationException ex) {
            code= ex.getCode();
        }
        assertEquals("Expected exception doesn't occurs",
                ReplyCode.KO_STREAM_DOES_NOT_EXIST.getCode(), (Integer) code);

        streamOperationsService.createStream(StreamsHelper.STREAM_NAME, StreamsHelper.COLUMNS);
        String queryId= streamOperationsService.addQuery(StreamsHelper.STREAM_NAME, StreamsHelper.QUERY);

        validation= new StreamNotExistsValidation(streamOperationsService);
        validation.validate(message);

    }


    @Test
    public void testStreamNotNullValidation()   throws Exception {
        StreamNameNotNullValidation validation= new StreamNameNotNullValidation();
        StratioStreamingMessage message= StreamsHelper.getSampleMessage();
        validation.validate(message);

        int code= -1;
        try {
            message.setStreamName(null);
            validation.validate(message);
        } catch (RequestValidationException ex) {
            code= ex.getCode();
        }
        assertEquals("Expected exception doesn't occurs",
                ReplyCode.KO_STREAM_OPERATION_NOT_ALLOWED.getCode(), (Integer) code);
    }


    @Test
    public void testUserDefinedStreamValidation()   throws Exception {
        streamOperationsService.createStream(StreamsHelper.STREAM_NAME, StreamsHelper.COLUMNS);
        String queryId= streamOperationsService.addQuery(StreamsHelper.STREAM_NAME, StreamsHelper.QUERY);

        UserDefinedStreamValidation validation= new UserDefinedStreamValidation(streamOperationsService);
        StratioStreamingMessage message= StreamsHelper.getSampleMessage();
        validation.validate(message);

        streamOperationsService.dropStream(StreamsHelper.STREAM_NAME);

        int code= -1;
        try {
            validation.validate(message);
        } catch (RequestValidationException ex) {
            code= ex.getCode();
        }
        assertEquals("Expected exception doesn't occurs",
                ReplyCode.KO_STREAM_OPERATION_NOT_ALLOWED.getCode(), (Integer) code);

    }

}