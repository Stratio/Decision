package com.stratio.streaming.functions.validator;

import org.wso2.siddhi.core.SiddhiManager;

import com.stratio.streaming.commons.constants.REPLY_CODES;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.exception.RequestValidationException;

public class StreamExistsValidation extends BaseSiddhiRequestValidation {

    private final static String STREAM_DOES_NOT_EXIST_MESSAGE = "Stream %s does not exists";

    public StreamExistsValidation(SiddhiManager sm) {
        super(sm);
    }

    @Override
    public void validate(StratioStreamingMessage request) throws RequestValidationException {
        if (request.getStreamName() != null && !"".equals(request.getStreamName())) {
            if (getSm().getStreamDefinition(request.getStreamName()) == null) {
                throw new RequestValidationException(REPLY_CODES.KO_STREAM_DOES_NOT_EXIST, String.format(
                        STREAM_DOES_NOT_EXIST_MESSAGE, request.getStreamName()));
            }
        }
    }
}
