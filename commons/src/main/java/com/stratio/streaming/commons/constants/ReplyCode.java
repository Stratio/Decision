package com.stratio.streaming.commons.constants;

public enum ReplyCode {
    OK(1, "Operation realized correctly."),
    KO_PARSER_ERROR(2, "Parser error."),
    KO_STREAM_ALREADY_EXISTS(3, "Stream already exists."),
    KO_STREAM_DOES_NOT_EXIST(4, "Stream does not exists."),
    KO_QUERY_ALREADY_EXISTS(5, "Query already exists."),
    KO_LISTENER_ALREADY_EXISTS(6, "Listener already exists."),
    KO_GENERAL_ERROR(7, "General error."),
    KO_COLUMN_ALREADY_EXISTS(8, "Column already exists."),
    KO_COLUMN_DOES_NOT_EXIST(9, "Column does not exists."),
    KO_LISTENER_DOES_NOT_EXIST(10, "Listener does not exists."),
    KO_QUERY_DOES_NOT_EXIST(11, "Query does not exists."),
    KO_STREAM_IS_NOT_USER_DEFINED(12, "Stream is not user defined."),
    KO_OUTPUTSTREAM_EXISTS_AND_DEFINITION_IS_DIFFERENT(
            13,
            "Output stream already exists and it´s definition is different."),
    KO_ACTION_ALREADY_ENABLED(14, "Action into stream already enabled."),
    KO_SOURCE_STREAM_DOES_NOT_EXIST(15, "Source stream in query does not exists."),
    KO_STREAM_OPERATION_NOT_ALLOWED(17, "Stream operation not allowed.");

    private final Integer code;
    private final String message;

    private ReplyCode(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public Integer getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

}
