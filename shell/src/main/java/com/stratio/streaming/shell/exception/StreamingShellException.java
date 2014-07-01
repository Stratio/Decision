package com.stratio.streaming.shell.exception;

public class StreamingShellException extends RuntimeException {

    private static final long serialVersionUID = -6008162825488430717L;

    public StreamingShellException(String message) {
        super(message);
    }

    public StreamingShellException(Throwable cause) {
        super(cause);
    }

    @Override
    public String toString() {
        String error = "";
        if (getCause() != null) {
            error = getCause().getMessage();
        } else {
            error = getMessage();
        }
        return "-> ".concat(error);
    }
}
