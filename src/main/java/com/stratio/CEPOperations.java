package com.stratio;


public enum CEPOperations {
    CREATE_OPERATION("CREATE"),
    ADD_QUERY_OPERATION("ADDQUERY"),
    LISTEN_OPERATION("LISTEN"),
    DROP_OPERATION("DROP"),
    INSERT_OPERATION("INSERT"),
    ALTER_OPERATION("ALTER"),
    LIST_OPERATION("LIST");

    private String stringValue;

    private CEPOperations(final String s) { stringValue = s; }
    public String toString() { return stringValue; }
}
