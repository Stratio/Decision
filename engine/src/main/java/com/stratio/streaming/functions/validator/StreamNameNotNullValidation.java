package com.stratio.streaming.functions.validator;

import com.stratio.streaming.commons.constants.ReplyCode;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.exception.RequestValidationException;

public class StreamNameNotNullValidation implements RequestValidation {

    private final static String STREAM_NAME_NOT_NULL = "Stream name cannot be null.";

    @Override
    public void validate(StratioStreamingMessage request) throws RequestValidationException {
        if (request.getStreamName() == null) {
            throw new RequestValidationException(ReplyCode.KO_STREAM_OPERATION_NOT_ALLOWED.getCode(),
                    STREAM_NAME_NOT_NULL);
        }
    }
}