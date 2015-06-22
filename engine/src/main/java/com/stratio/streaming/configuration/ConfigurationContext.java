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
package com.stratio.streaming.configuration;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class ConfigurationContext {

    /**
     * MANDATORY PROPERTIES *
     */

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

    /**
     * OPTIONAL PROPERTIES *
     */

    private final List<String> elasticSearchHosts;
    private final String elasticSearchClusterName;

    private final List<String> mongoHosts;
    private final String mongoUsername;
    private final String mongoPassword;

    public enum ConfigurationKeys {
        CASSANDRA_HOSTS("cassandra.hosts"),
        KAFKA_HOSTS("kafka.hosts"),
        ZOOKEEPER_HOSTS("zookeeper.hosts"),
        FAILOVER_ENABLED("failover.enabled"),
        FAILOVER_PERIOD("failover.period"),
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
        ELASTICSEARCH_HOST("elasticsearch.hosts"),
        ELASTICSEARCH_CLUSTER_NAME("elasticsearch.clusterName"),
        MONGO_HOST("mongo.hosts"),
        MONGO_USER("mongo.user"),
        MONGO_PASSWORD("mongo.password");

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

        this.cassandraHosts = config.getStringList(ConfigurationKeys.CASSANDRA_HOSTS.getKey());
        this.kafkaHosts = config.getStringList(ConfigurationKeys.KAFKA_HOSTS.getKey());
        this.kafkaConsumerBrokerHost = kafkaHosts.get(0).split(":")[0];
        this.kafkaConsumerBrokerPort = Integer.parseInt(kafkaHosts.get(0).split(":")[1]);
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

        this.elasticSearchHosts = (List<String>) this.getListOrNull(ConfigurationKeys.ELASTICSEARCH_HOST.getKey(), config);
        this.elasticSearchClusterName = (String) this.getValueOrNull(ConfigurationKeys.ELASTICSEARCH_CLUSTER_NAME.getKey(), config);

        this.mongoHosts = (List<String>) this.getListOrNull(ConfigurationKeys.MONGO_HOST.getKey(), config);
        this.mongoUsername = (String) this.getValueOrNull(ConfigurationKeys.MONGO_USER.getKey(), config);
        this.mongoPassword = (String) this.getValueOrNull(ConfigurationKeys.MONGO_PASSWORD.getKey(), config);

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
