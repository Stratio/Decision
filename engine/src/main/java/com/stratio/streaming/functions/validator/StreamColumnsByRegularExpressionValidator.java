package com.stratio.streaming.functions.validator;

import com.stratio.streaming.commons.constants.StreamAction;
import com.stratio.streaming.commons.messages.ColumnNameTypeValue;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.exception.RequestValidationException;

public class StreamColumnsByRegularExpressionValidator extends BaseRegularExpressionValidator {

    private final StreamAction streamAction;

    public StreamColumnsByRegularExpressionValidator(String regularExpression, StreamAction streamAction) {
        super(regularExpression);
        this.streamAction = streamAction;
    }

    @Override
    public void validate(StratioStreamingMessage request) throws RequestValidationException {
        for (ColumnNameTypeValue column : request.getColumns()) {
            stringMatch(column.getColumn());
        }
    }

    @Override
    public String getHumanReadableErrorMessage(String stringValidated) {
        return String.format("Column name %s is not compatible with %s action.", stringValidated, streamAction);
    }

}
