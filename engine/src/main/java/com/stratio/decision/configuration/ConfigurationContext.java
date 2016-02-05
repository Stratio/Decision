/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.decision.configuration;

import com.stratio.decision.commons.constants.InternalTopic;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ConfigurationContext {

    /**
     * MANDATORY PROPERTIES *
     */
    private final String groupId;

    private final List<String> cassandraHosts;
    private final List<String> kafkaHosts;
    private final String kafkaConsumerBrokerHost;
    private final int kafkaConsumerBrokerPort;
    private final List<String> zookeeperHosts;

    private final boolean failOverEnabled;
    private final long failOverPeriod;

    private final boolean auditEnabled;
    private final boolean statsEnabled;
    private final boolean printStreams;

    private final long streamingBatchTime;
    private final long internalStreamingBatchTime;
    private final int kafkaReplicationFactor;
    private final int kafkaPartitions;
    private final int kafkaSessionTimeout;
    private final int kafkaConnectionTimeout;

    private final String sparkHost;
    private final String internalSparkHost;

    private final boolean clusteringEnabled;
    private final List<String> clusterGroups;
    private final boolean allAckEnabled;
    private final int ackTimeout;

    /**
     * OPTIONAL PROPERTIES *
     */

    private final List<String> dataTopics;

    private final List<String> elasticSearchHosts;
    private final String elasticSearchClusterName;

    private final String solrHosts;
    private final Boolean solrCloud;
    private final String solrDataDir;

    private final List<String> mongoHosts;
    private final String mongoUsername;
    private final String mongoPassword;


    public enum ConfigurationKeys {
        CASSANDRA_HOSTS("cassandra.hosts"),
        KAFKA_HOSTS("kafka.hosts"),
        ZOOKEEPER_HOSTS("zookeeper.hosts"),
        FAILOVER_ENABLED("clustering.failoverEnabled"),
        FAILOVER_PERIOD("clustering.failoverPeriod"),
        AUDIT_ENABLED("auditEnabled"),
        STATS_ENABLED("statsEnabled"),
        PRINT_STREAMS("printStreams"),
        STREAMING_BATCH_TIME("spark.streamingBatchTime"),
        INTERNAL_STREAMING_BATCH_TIME("spark.internalStreamingBatchTime"),
        SPARK_HOST("spark.host"),
        INTERNAL_SPARK_HOST("spark.internalHost"),
        KAFKA_REPLICATION_FACTOR("kafka.replicationFactor"),
        KAFKA_PARTITIONS("kafka.partitions"),
        KAFKA_SESSION_TIMEOUT("kafka.sessionTimeout"),
        KAFKA_CONNECTION_TIMEOUT("kafka.connectionTimeout"),
        DATA_TOPICS("clustering.dataTopics"),
        ELASTICSEARCH_HOST("elasticsearch.hosts"),
        ELASTICSEARCH_CLUSTER_NAME("elasticsearch.clusterName"),
        SOLR_HOST("solr.hosts"),
        SOLR_CLOUD("solr.cloud"),
        SOLR_DATADIR("solr.dataDir"),
        MONGO_HOST("mongo.hosts"),
        MONGO_USER("mongo.user"),
        MONGO_PASSWORD("mongo.password"),
        CLUSTERING_GROUP_ID("clustering.groupId"),
        CLUSTERING_ENABLED("clustering.enabled"),
        CLUSTERING_GROUPS("clustering.clusterGroups"),
        CLUSTERING_ALL_ACK_ENABLED("clustering.allAckEnabled"),
        CLUSTERING_ACK_TIMEOUT("clustering.ackTimeout");

        private final String key;

        private ConfigurationKeys(String key) {
            this.key = key;
        }

        public String getKey() {
            return key;
        }

    }

    public ConfigurationContext() {
        Config config = ConfigFactory.load("config");

        this.groupId =  config.getString(ConfigurationKeys.CLUSTERING_GROUP_ID.getKey());

        this.kafkaHosts = config.getStringList(ConfigurationKeys.KAFKA_HOSTS.getKey());
        this.kafkaConsumerBrokerHost = kafkaHosts.get(0).split(":")[0];
        this.kafkaConsumerBrokerPort = Integer.parseInt(kafkaHosts.get(0).split(":")[1]);

        List<String> dataTopics = (List<String>) this.getListOrNull(ConfigurationKeys.DATA_TOPICS.getKey(),
                config);

        if (dataTopics != null) {

            String separator = "_";

            this.dataTopics = dataTopics.stream().map(topic -> InternalTopic.TOPIC_DATA.getTopicName().concat
                    (separator).concat(topic))
            .collect(Collectors.toList());

        } else {
            dataTopics = new ArrayList<>();
            dataTopics.add(InternalTopic.TOPIC_DATA.getTopicName());
            this.dataTopics = dataTopics;
        }


        this.zookeeperHosts = config.getStringList(ConfigurationKeys.ZOOKEEPER_HOSTS.getKey());
        this.sparkHost = config.getString(ConfigurationKeys.SPARK_HOST.getKey());
        this.internalSparkHost = config.getString(ConfigurationKeys.INTERNAL_SPARK_HOST.getKey());

        this.failOverEnabled = config.getBoolean(ConfigurationKeys.FAILOVER_ENABLED.getKey());
        this.failOverPeriod = config.getDuration(ConfigurationKeys.FAILOVER_PERIOD.getKey(), TimeUnit.MILLISECONDS);
        this.auditEnabled = config.getBoolean(ConfigurationKeys.AUDIT_ENABLED.getKey());
        this.statsEnabled = config.getBoolean(ConfigurationKeys.STATS_ENABLED.getKey());
        this.printStreams = config.getBoolean(ConfigurationKeys.PRINT_STREAMS.getKey());

        this.streamingBatchTime = config.getDuration(ConfigurationKeys.STREAMING_BATCH_TIME.getKey(),
                TimeUnit.MILLISECONDS);
        this.internalStreamingBatchTime = config.getDuration(ConfigurationKeys.INTERNAL_STREAMING_BATCH_TIME.getKey(),
                TimeUnit.MILLISECONDS);
        this.kafkaReplicationFactor = config.getInt(ConfigurationKeys.KAFKA_REPLICATION_FACTOR.getKey());
        this.kafkaPartitions = config.getInt(ConfigurationKeys.KAFKA_PARTITIONS.getKey());
        this.kafkaSessionTimeout = config.getInt(ConfigurationKeys.KAFKA_SESSION_TIMEOUT.getKey());
        this.kafkaConnectionTimeout = config.getInt(ConfigurationKeys.KAFKA_CONNECTION_TIMEOUT.getKey());

        this.cassandraHosts = (List<String>) this.getListOrNull(ConfigurationKeys.CASSANDRA_HOSTS.getKey(), config);

        this.elasticSearchHosts = (List<String>) this.getListOrNull(ConfigurationKeys.ELASTICSEARCH_HOST.getKey(), config);
        this.elasticSearchClusterName = (String) this.getValueOrNull(ConfigurationKeys.ELASTICSEARCH_CLUSTER_NAME.getKey(), config);

        this.solrHosts = (String) this.getValueOrNull(ConfigurationKeys.SOLR_HOST.getKey(), config);
        this.solrCloud = (Boolean) this.getValueOrNull(ConfigurationKeys.SOLR_CLOUD.getKey(), config);
        this.solrDataDir = (String) this.getValueOrNull(ConfigurationKeys.SOLR_DATADIR.getKey(), config);

        this.mongoHosts = (List<String>) this.getListOrNull(ConfigurationKeys.MONGO_HOST.getKey(), config);
        this.mongoUsername = (String) this.getValueOrNull(ConfigurationKeys.MONGO_USER.getKey(), config);
        this.mongoPassword = (String) this.getValueOrNull(ConfigurationKeys.MONGO_PASSWORD.getKey(), config);

        this.clusterGroups = (List<String>) this.getListOrNull(ConfigurationKeys.CLUSTERING_GROUPS.getKey(),
                config);
        this.allAckEnabled = config.getBoolean(ConfigurationKeys.CLUSTERING_ALL_ACK_ENABLED.getKey());
        this.ackTimeout = config.getInt(ConfigurationKeys.CLUSTERING_ACK_TIMEOUT.getKey());
        this.clusteringEnabled = config.getBoolean(ConfigurationKeys.CLUSTERING_ENABLED.getKey());

    }

    public String getGroupId() {
        return groupId;
    }

    public List<String> getCassandraHosts() {
        return cassandraHosts;
    }

    public String getCassandraHostsQuorum() {
        return StringUtils.join(cassandraHosts, ",");
    }

    public List<String> getKafkaHosts() {
        return kafkaHosts;
    }

    public String getKafkaConsumerBrokerHost() {
        return kafkaConsumerBrokerHost;
    }

    public int getKafkaConsumerBrokerPort() {
        return kafkaConsumerBrokerPort;
    }

    public List<String> getDataTopics() {
        return dataTopics;
    }

    public List<String> getZookeeperHosts() {
        return zookeeperHosts;
    }

    public String getZookeeperHostsQuorum() {
        return StringUtils.join(zookeeperHosts, ",");
    }

    public String getKafkaHostsQuorum() {
        return StringUtils.join(kafkaHosts, ",");
    }

    public boolean isFailOverEnabled() {
        return failOverEnabled;
    }

    public long getFailOverPeriod() {
        return failOverPeriod;
    }

    public boolean isAuditEnabled() {
        return auditEnabled;
    }

    public boolean isStatsEnabled() {
        return statsEnabled;
    }

    public boolean isPrintStreams() {
        return printStreams;
    }

    public long getStreamingBatchTime() {
        return streamingBatchTime;
    }

    public String getSparkHost() {
        return sparkHost;
    }

    public int getKafkaReplicationFactor() {
        return kafkaReplicationFactor;
    }

    public int getKafkaPartitions() {
        return kafkaPartitions;
    }

    public int getKafkaSessionTimeout() {
        return kafkaSessionTimeout;
    }

    public int getKafkaConnectionTimeout() {
        return kafkaConnectionTimeout;
    }

    public List<String> getElasticSearchHosts() {
        return elasticSearchHosts;
    }

    public String getElasticSearchClusterName() {
        return elasticSearchClusterName;
    }

    public String getSolrHosts() {
        return solrHosts;
    }

    public Boolean getSolrCloud() {
        return solrCloud;
    }

    public String getSolrDataDir() {
        return solrDataDir;
    }

    public List<String> getMongoHosts() {
        return mongoHosts;
    }

    public String getMongoUsername() {
        return mongoUsername;
    }

    public String getMongoPassword() {
        return mongoPassword;
    }

    public long getInternalStreamingBatchTime() {
        return internalStreamingBatchTime;
    }

    public String getInternalSparkHost() {
        return internalSparkHost;
    }

    public List<String> getClusterGroups() {
        return clusterGroups;
    }

    public boolean isAllAckEnabled() {
        return allAckEnabled;
    }

    public int getAckTimeout() {
        return ackTimeout;
    }

    public boolean isClusteringEnabled() {
        return clusteringEnabled;
    }

    private Object getValueOrNull(String key, Config config) {
        if (config.hasPath(key)) {
            return config.getAnyRef(key);
        } else {
            return null;
        }
    }

    private Object getListOrNull(String key, Config config) {
        if (config.hasPath(key)) {
            return config.getAnyRefList(key);
        } else {
            return null;
        }
    }
}
