package com.stratio.streaming.functions.validator;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.stratio.streaming.commons.constants.ReplyCode;
import com.stratio.streaming.exception.RequestValidationException;

public abstract class BaseRegularExpressionValidator implements RequestValidation {

    private final Pattern pattern;

    public BaseRegularExpressionValidator(String regularExpression) {
        pattern = Pattern.compile(regularExpression);
    }

    public void stringMatch(String string) throws RequestValidationException {
        Matcher matcher = pattern.matcher(string);
        if (!matcher.matches()) {
            throw new RequestValidationException(ReplyCode.KO_STREAM_OPERATION_NOT_ALLOWED.getCode(),
                    getHumanReadableErrorMessage(string));
        }
    }

    public abstract String getHumanReadableErrorMessage(String stringValidated);

}
