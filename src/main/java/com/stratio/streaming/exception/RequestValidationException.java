package com.stratio.streaming.exception;

public class RequestValidationException extends Exception {

    private static final long serialVersionUID = 1191962540939323833L;

    private final int code;

    public RequestValidationException(int code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public RequestValidationException(int code, String message) {
        super(message);
        this.code = code;
    }

    public RequestValidationException(int code, Throwable cause) {
        super(cause);
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
