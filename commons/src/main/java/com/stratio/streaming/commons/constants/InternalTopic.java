package com.stratio.streaming.commons.constants;

public enum InternalTopic {
    TOPIC_REQUEST("stratio_streaming_requests"), TOPIC_DATA("stratio_streaming_data"), TOPIC_ACTION(
            "stratio_streaming_action");

    private final String topicName;

    private InternalTopic(String topicName) {
        this.topicName = topicName;
    }

    public String getTopicName() {
        return topicName;
    }
}
