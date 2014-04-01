package com.stratio.streaming.commons.exceptions;

public class StratioEngineOperationException extends StratioStreamingException {
    public StratioEngineOperationException(String message){
        super(message, "StratioEngineOperationException");
    }
}
