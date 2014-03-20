package com.stratio;

public enum TopicNames {
    STREAMING_REQUESTS_TOPIC("stratio_streaming_requests"),
    STREAMING_LIST_TOPIC("stratio_streaming_list_streams");

    private String stringValue;

    private TopicNames(final String s) { stringValue = s; }
    public String toString() { return stringValue; }
}
