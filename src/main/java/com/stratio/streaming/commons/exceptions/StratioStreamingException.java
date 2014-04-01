package com.stratio.streaming.commons.exceptions;

public class StratioStreamingException extends Exception {

    private String errorCode="Stratio_Streaming_Exception";

    public StratioStreamingException(String message, String errorCode){
        super(message);
        this.errorCode=errorCode;
    }

    public String getErrorCode(){
        return this.errorCode;
    }


}
