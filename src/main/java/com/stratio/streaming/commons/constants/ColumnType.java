package com.stratio.streaming.commons.constants;

public enum ColumnType {
    STRING("STRING"),
    BOOLEAN("BOOLEAN"),
    DOUBLE("DOUBLE"),
    INTEGER("INTEGER"),
    LONG("LONG"),
    FLOAT("FLOAT");

    private String value;

    private ColumnType(String value) {
        this.value = value;
    }

    public String getValue() {
       return value;
    }
}