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

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class ConfigurationContext {

    /** MANDATORY PROPERTIES **/

    private final List<String> cassandraHosts;
    private final List<String> kafkaHosts;
    private final List<String> zookeeperHosts;

    private final boolean failOverEnabled;
    private final long failOverPeriod;

    private final boolean auditEnabled;
    private final boolean statsEnabled;
    private final boolean printStreams;

    private final long streamingBatchTime;
    private final int kafkaReplicationFactor;
    private final int kafkaPartitions;
    private final int kafkaSessionTimeout;
    private final int kafkaConnectionTimeout;

    private final String sparkHost;

    /** OPTIONAL PROPERTIES **/

    private final String elasticSearchHost;
    private final int elasticSearchPort;

    private final String mongoHost;
    private final int mongoPort;
    private final String mongoUsername;
    private final String mongoPassword;

    public enum ConfigurationKeys {
        CASSANDRA_HOSTS("cassandra.hosts"), KAFKA_HOSTS("kafka.hosts"), ZOOKEEPER_HOSTS("zookeeper.hosts"), FAILOVER_ENABLED(
                "failover.enabled"), FAILOVER_PERIOD("failover.period"), AUDIT_ENABLED("auditEnabled"), STATS_ENABLED(
                "statsEnabled"), PRINT_STREAMS("printStreams"), STREAMING_BATCH_TIME("spark.streamingBatchTime"), SPARK_HOST(
                "spark.host"), KAFKA_REPLICATION_FACTOR("kafka.replicationFactor"), KAFKA_PARTITIONS("kafka.partitions"), KAFKA_SESSION_TIMEOUT(
                "kafka.sessionTimeout"), KAFKA_CONNECTION_TIMEOUT("kafka.connectionTimeout"), ELASTICSEARCH_HOST(
                "elasticsearch.host"), ELASTICSEARCH_PORT("elasticsearch.port"), MONGO_HOST("mongo.host"), MONGO_PORT(
                "mongo.port"), MONGO_USER("mongo.user"), MONGO_PASSWORD("mongo.password");

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
        this.zookeeperHosts = config.getStringList(ConfigurationKeys.ZOOKEEPER_HOSTS.getKey());
        this.sparkHost = config.getString(ConfigurationKeys.SPARK_HOST.getKey());

        this.failOverEnabled = config.getBoolean(ConfigurationKeys.FAILOVER_ENABLED.getKey());
        this.failOverPeriod = config.getDuration(ConfigurationKeys.FAILOVER_PERIOD.getKey(), TimeUnit.MILLISECONDS);
        this.auditEnabled = config.getBoolean(ConfigurationKeys.AUDIT_ENABLED.getKey());
        this.statsEnabled = config.getBoolean(ConfigurationKeys.STATS_ENABLED.getKey());
        this.printStreams = config.getBoolean(ConfigurationKeys.PRINT_STREAMS.getKey());

        this.streamingBatchTime = config.getDuration(ConfigurationKeys.STREAMING_BATCH_TIME.getKey(),
                TimeUnit.MILLISECONDS);
        this.kafkaReplicationFactor = config.getInt(ConfigurationKeys.KAFKA_REPLICATION_FACTOR.getKey());
        this.kafkaPartitions = config.getInt(ConfigurationKeys.KAFKA_PARTITIONS.getKey());
        this.kafkaSessionTimeout = config.getInt(ConfigurationKeys.KAFKA_SESSION_TIMEOUT.getKey());
        this.kafkaConnectionTimeout = config.getInt(ConfigurationKeys.KAFKA_CONNECTION_TIMEOUT.getKey());

        this.elasticSearchHost = (String) this.getValueOrNull(ConfigurationKeys.ELASTICSEARCH_HOST.getKey(), config);
        this.elasticSearchPort = (Integer) this.getValueOrNull(ConfigurationKeys.ELASTICSEARCH_PORT.getKey(), config);

        this.mongoHost = (String) this.getValueOrNull(ConfigurationKeys.MONGO_HOST.getKey(), config);
        this.mongoPort = (Integer) this.getValueOrNull(ConfigurationKeys.MONGO_PORT.getKey(), config);
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

    public String getElasticSearchHost() {
        return elasticSearchHost;
    }

    public int getElasticSearchPort() {
        return elasticSearchPort;
    }

    public String getMongoHost() {
        return mongoHost;
    }

    public int getMongoPort() {
        return mongoPort;
    }

    public String getMongoUsername() {
        return mongoUsername;
    }

    public String getMongoPassword() {
        return mongoPassword;
    }

    private Object getValueOrNull(String key, Config config) {
        if (config.hasPath(key)) {
            return config.getAnyRef(key);
        } else {
            return null;
        }
    }
}
