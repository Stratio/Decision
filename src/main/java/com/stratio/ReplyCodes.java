package com.stratio;


public enum ReplyCodes {
    OK("1"),
    KO_PARSER_ERROR("2"),
    KO_STREAM_ALREADY_EXISTS("3"),
    KO_STREAM_DOES_NOT_EXIST("4"),
    KO_QUERY_ALREADY_EXISTS("5"),
    KO_LISTENER_ALREADY_EXISTS("6"),
    KO_GENERAL_ERROR("7"),
    KO_COLUMN_ALREADY_EXISTS("8"),
    KO_COLUMN_DOES_NOT_EXISTS("9");

    private String stringValue;

    private ReplyCodes(final String s) { stringValue = s; }
    public String toString() { return stringValue; }
}
