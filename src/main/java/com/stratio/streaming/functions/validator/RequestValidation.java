package com.stratio.streaming.functions.validator;

import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.exception.RequestValidationException;

public interface RequestValidation {

    public void validate(StratioStreamingMessage request) throws RequestValidationException;

}
