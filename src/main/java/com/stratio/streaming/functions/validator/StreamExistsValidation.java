package com.stratio.streaming.functions.validator;

import org.wso2.siddhi.core.SiddhiManager;

import com.stratio.streaming.commons.constants.REPLY_CODES;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.exception.RequestValidationException;

public class StreamExistsValidation extends BaseSiddhiRequestValidation {

    private final static String STREAM_ALREADY_EXISTS_MESSAGE = "Stream %s already exists";

    public StreamExistsValidation(SiddhiManager sm) {
        super(sm);
    }

    @Override
    public void validate(StratioStreamingMessage request) throws RequestValidationException {
        if (request.getStreamName() != null && !"".equals(request.getStreamName())) {
            if (getSm().getStreamDefinition(request.getStreamName()) != null) {
                throw new RequestValidationException(REPLY_CODES.KO_STREAM_ALREADY_EXISTS, String.format(
                        STREAM_ALREADY_EXISTS_MESSAGE, request.getStreamName()));
            }
        }
    }
}
