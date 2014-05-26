package com.stratio.streaming.functions.validator;

import org.wso2.siddhi.core.SiddhiManager;

import com.stratio.streaming.commons.constants.REPLY_CODES;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.exception.RequestValidationException;
import com.stratio.streaming.utils.SiddhiUtils;

public class StreamAllowedValidation extends BaseSiddhiRequestValidation {

    private final static String STREAM_OPERATION_NOT_ALLOWED = "Operation %s not allowed in stream %s";

    public StreamAllowedValidation(SiddhiManager sm) {
        super(sm);
    }

    @Override
    public void validate(StratioStreamingMessage request) throws RequestValidationException {
        if (!SiddhiUtils.isStreamAllowedForThisOperation(request.getStreamName(), request.getOperation())) {
            throw new RequestValidationException(REPLY_CODES.KO_STREAM_OPERATION_NOT_ALLOWED, String.format(
                    STREAM_OPERATION_NOT_ALLOWED, request.getOperation(), request.getStreamName()));
        }
    }
}
