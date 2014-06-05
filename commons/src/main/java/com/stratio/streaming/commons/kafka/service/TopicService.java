package com.stratio.streaming.commons.kafka.service;

public interface TopicService {

    void createTopicIfNotExist(String topic, int replicationFactor, int partitions);

    void createOrUpdateTopic(String topic, int replicationFactor, int partitions);

    Integer getNumPartitionsForTopic(String topic);

}
