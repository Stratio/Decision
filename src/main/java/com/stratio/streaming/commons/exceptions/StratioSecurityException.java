package com.stratio.streaming.commons.exceptions;

public class StratioSecurityException extends StratioStreamingException {
    public StratioSecurityException(String message){
        super(message, "StratioSecurityException");
    }
}
