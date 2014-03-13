package com.stratio.bus

import kafka.utils.{ZkUtils, ZKStringSerializer}
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.serialize.ZkSerializer
import kafka.javaapi.consumer.SimpleConsumer
import kafka.javaapi.TopicMetadataRequest
import kafka.admin.AdminUtils
import scala.collection.JavaConverters._
import kafka.common.Topic

object KafkaTopicUtils {
  def createTopic(zookeeperCluster: String,
                  topic: String,
                  numPartitions: Integer,
                  replicationFactor: Integer) = {
    val zkSerializer = new ZkSerializer {
      def serialize(p1: Object)  = ZKStringSerializer.serialize(p1)

      def deserialize(p1: Array[Byte]): AnyRef = ZKStringSerializer.deserialize(p1)
    }
    val zkClient = new ZkClient(zookeeperCluster, 30000, 30000)
    zkClient.setZkSerializer(zkSerializer)
    createOrUpdateTopic(zkClient, topic, numPartitions, replicationFactor)
    zkClient.close()

    true
  }

  def createOrUpdateTopic(zkClient: ZkClient,
                          topic: String,
                          numPartitions: Int,
                          replicationFactor: Int) {
    Topic.validate(topic)
    val brokerList = ZkUtils.getSortedBrokerList(zkClient)
    val partitionReplicaAssignment = AdminUtils.assignReplicasToBrokers(brokerList, numPartitions, replicationFactor,0,0)
    System.out.println("Replica assignment list for " + topic + " is " + partitionReplicaAssignment.toString())
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(topic, partitionReplicaAssignment, zkClient, true)

    true
  }

  def getNumPartitionsForTopic(brokerList: String, brokerPort: Int, topic: String) {
    val consumer  = new SimpleConsumer(brokerList, brokerPort, 100000, 64 * 1024, "topicLookup")
    val topics = List(topic).asJava
    val topicMetaDataRequest = new TopicMetadataRequest(topics)
    val topicMetadataList = (consumer.send(topicMetaDataRequest)).topicsMetadata
    val numPartitions = topicMetadataList.get(0).partitionsMetadata.size()

    consumer.close()

    return numPartitions
  }
}
