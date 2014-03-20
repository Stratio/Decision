package com.stratio;


public enum Paths {
    ZK_BASE_PATH("/stratio/streaming");

    private String stringValue;

    private Paths(final String s) { stringValue = s; }
    public String toString() { return stringValue; }
}
