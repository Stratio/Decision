/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.decision.commons.kafka.service;

import java.io.IOException;
import java.util.Arrays;

import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.decision.commons.kafka.serializer.ZkStringSerializer;

import kafka.admin.AdminUtils;
import kafka.common.Topic;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.utils.ZkUtils;
import scala.collection.Map;
import scala.collection.Seq;

public class KafkaTopicService implements TopicService {

    private final static Logger logger = LoggerFactory.getLogger(KafkaTopicService.class);

    private final ZkClient zkClient;
    private final SimpleConsumer simpleConsumer;

    private final static int CONSUMER_TIMEOUT = 100000;
    private final static int CONSUMER_BUFFER_SIZE = 64 * 1024;
    private final static String CONSUMER_CLIENT_ID = "leaderLookup";

    public KafkaTopicService(String zokeeperCluster, String broker, int brokerPort, int connectionTimeout,
                             int sessionTimeout) {
        this.zkClient = new ZkClient(zokeeperCluster, sessionTimeout, connectionTimeout, new ZkStringSerializer());
        this.simpleConsumer = new SimpleConsumer(broker, brokerPort, CONSUMER_TIMEOUT, CONSUMER_BUFFER_SIZE,
                CONSUMER_CLIENT_ID);
    }

    @Override
    public void createTopicIfNotExist(String topic, int replicationFactor, int partitions) {
        if (!AdminUtils.topicExists(zkClient, topic)) {
            createOrUpdateTopic(topic, replicationFactor, partitions);
        } else {
            logger.info("Topic {} already exists", topic);
        }
    }

    @Override
    public void createOrUpdateTopic(String topic, int replicationFactor, int partitions) {
        logger.debug("Creating topic {} with replication {} and {} partitions", topic, replicationFactor, partitions);
        Topic.validate(topic);
        Seq<Object> brokerList = ZkUtils.getSortedBrokerList(zkClient);
        Map<Object, Seq<Object>> partitionReplicaAssignment = AdminUtils.assignReplicasToBrokers(brokerList,
                partitions, replicationFactor, AdminUtils.assignReplicasToBrokers$default$4(),
                AdminUtils.assignReplicasToBrokers$default$5());
        AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, partitionReplicaAssignment,
                AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK$default$4(),
                AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK$default$5());
        logger.debug("Topic {} created", topic);
    }

    @Override
    public Integer getNumPartitionsForTopic(String topic){
        TopicMetadataRequest topicRequest = new TopicMetadataRequest(Arrays.asList(topic));
        TopicMetadataResponse topicResponse = simpleConsumer.send(topicRequest);
        for (TopicMetadata topicMetadata : topicResponse.topicsMetadata()) {
            if (topic.equals(topicMetadata.topic())) {
                int partitionSize = topicMetadata.partitionsMetadata().size();
                logger.debug("Partition size found ({}) for {} topic", partitionSize, topic);
                return partitionSize;
            }
        }
        logger.warn("Metadata info not found!. TOPIC {}", topic);
        return null;
    }

    public void deleteTopics(){
        zkClient.deleteRecursive("/brokers/topics");
    }

    @Override
    public void close() throws IOException {
        zkClient.close();
        simpleConsumer.close();
    }
}
