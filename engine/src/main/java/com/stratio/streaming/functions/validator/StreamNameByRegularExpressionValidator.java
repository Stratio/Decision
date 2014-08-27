package com.stratio.streaming.functions.validator;

import com.stratio.streaming.commons.constants.StreamAction;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.exception.RequestValidationException;

public class StreamNameByRegularExpressionValidator extends BaseRegularExpressionValidator {

    private final StreamAction streamAction;

    public StreamNameByRegularExpressionValidator(String regularExpression, StreamAction streamAction) {
        super(regularExpression);
        this.streamAction = streamAction;
    }

    @Override
    public void validate(StratioStreamingMessage request) throws RequestValidationException {
        stringMatch(request.getStreamName());
    }

    @Override
    public String getHumanReadableErrorMessage(String stringValidated) {
        return String.format("Stream name %s is not compatible with %s action.", stringValidated, streamAction);
    }

}
