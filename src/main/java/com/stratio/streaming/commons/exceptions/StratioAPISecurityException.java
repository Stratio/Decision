package com.stratio.streaming.commons.exceptions;

public class StratioAPISecurityException extends StratioStreamingException {
    public StratioAPISecurityException(String message){
        super(message, "StratioAPISecurityException");
    }
}
