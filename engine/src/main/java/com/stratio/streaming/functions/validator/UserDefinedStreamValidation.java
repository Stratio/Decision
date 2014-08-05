package com.stratio.streaming.functions.validator;

import com.stratio.streaming.commons.constants.REPLY_CODES;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.exception.RequestValidationException;
import com.stratio.streaming.service.StreamOperationService;

public class UserDefinedStreamValidation extends BaseSiddhiRequestValidation {

    public UserDefinedStreamValidation(StreamOperationService streamOperationService) {
        super(streamOperationService);
    }

    private final static String INTERNAL_STREAM_DROP_NOT_ALLOWED = "Cannot delete the internal stream %s";

    @Override
    public void validate(StratioStreamingMessage request) throws RequestValidationException {
        if (!request.isUserDefined()) {
            throw new RequestValidationException(REPLY_CODES.KO_STREAM_OPERATION_NOT_ALLOWED, String.format(
                    INTERNAL_STREAM_DROP_NOT_ALLOWED, request.getStreamName()));
        }

    }

}
