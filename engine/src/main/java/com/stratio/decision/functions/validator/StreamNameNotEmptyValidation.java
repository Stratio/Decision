package com.stratio.decision.functions.validator;

import com.stratio.decision.commons.constants.ReplyCode;
import com.stratio.decision.commons.messages.StratioStreamingMessage;
import com.stratio.decision.exception.RequestValidationException;

/**
 * Created by ajnavarro on 22/01/15.
 */
public class StreamNameNotEmptyValidation implements RequestValidation {

    private final static String STREAM_NAME_NOT_EMPTY = "Stream name cannot be empty.";

    @Override
    public void validate(StratioStreamingMessage request) throws RequestValidationException {
        if (request.getStreamName() == null || request.getStreamName().equals("")) {
            throw new RequestValidationException(ReplyCode.KO_STREAM_OPERATION_NOT_ALLOWED.getCode(),
                    STREAM_NAME_NOT_EMPTY);
        }
    }
}