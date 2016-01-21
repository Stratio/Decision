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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import com.stratio.decision.drools.configuration.DroolsConfigurationBean;
import com.stratio.decision.drools.configuration.DroolsConfigurationGroupBean;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

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

    private final String solrHosts;
    private final Boolean solrCloud;
    private final String solrDataDir;

    private final List<String> mongoHosts;
    private final String mongoUsername;
    private final String mongoPassword;

    private final DroolsConfigurationBean droolsConfiguration;

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
        SOLR_HOST("solr.hosts"),
        SOLR_CLOUD("solr.cloud"),
        SOLR_DATADIR("solr.dataDir"),
        MONGO_HOST("mongo.hosts"),
        MONGO_USER("mongo.user"),
        MONGO_PASSWORD("mongo.password"),

        // Drools Config
        DROOLS_GROUP("drools.groups"),
        DROOLS_GROUP_NAME("name"),
        DROOLS_GROUP_SESSION("sessionName"),
        DROOLS_GROUP_GROUP_ID("groupId"),
        DROOLS_GROUP_ARTIFACT_ID("artifactId"),
        DROOLS_GROUP_VERSION("version"),
        DROOLS_GROUP_SCAN_TIME("scanFrequency"),
        DROOLS_GROUP_SESSION_TYPE("sessionType"),
        ;

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
        Config c = ConfigFactory.load("drools");

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

        this.cassandraHosts = (List<String>) this.getListOrNull(ConfigurationKeys.CASSANDRA_HOSTS.getKey(), config);

        this.elasticSearchHosts = (List<String>) this.getListOrNull(ConfigurationKeys.ELASTICSEARCH_HOST.getKey(), config);
        this.elasticSearchClusterName = (String) this.getValueOrNull(ConfigurationKeys.ELASTICSEARCH_CLUSTER_NAME.getKey(), config);

        this.solrHosts = (String) this.getValueOrNull(ConfigurationKeys.SOLR_HOST.getKey(), config);
        this.solrCloud = (Boolean) this.getValueOrNull(ConfigurationKeys.SOLR_CLOUD.getKey(), config);
        this.solrDataDir = (String) this.getValueOrNull(ConfigurationKeys.SOLR_DATADIR.getKey(), config);

        this.mongoHosts = (List<String>) this.getListOrNull(ConfigurationKeys.MONGO_HOST.getKey(), config);
        this.mongoUsername = (String) this.getValueOrNull(ConfigurationKeys.MONGO_USER.getKey(), config);
        this.mongoPassword = (String) this.getValueOrNull(ConfigurationKeys.MONGO_PASSWORD.getKey(), config);

        // Adding Drools config
        this.droolsConfiguration = new DroolsConfigurationBean();
        Config droolsGroupsConfig = ConfigFactory.load("drools");
        droolsConfiguration.setGroups(getDroolsConfigurationGroup(droolsGroupsConfig));

    }

    private Map<String, DroolsConfigurationGroupBean> getDroolsConfigurationGroup(Config droolsConfig)  {

        Map<String, DroolsConfigurationGroupBean> groups= new HashMap<>();

        if (!droolsConfig.hasPath(ConfigurationKeys.DROOLS_GROUP.getKey())){
            return groups;
        }
        List list = droolsConfig.getConfigList(ConfigurationKeys.DROOLS_GROUP.getKey());
        Config groupConfig;

        for (int i=0; i<list.size(); i++){

            groupConfig = (Config) list.get(i);

            DroolsConfigurationGroupBean g= new DroolsConfigurationGroupBean();

            g.setSessionName((String) this.getValueOrNull(ConfigurationKeys.DROOLS_GROUP_SESSION.getKey(), groupConfig));
            g.setGroupId((String) this.getValueOrNull(ConfigurationKeys.DROOLS_GROUP_GROUP_ID.getKey(), groupConfig));
            g.setArtifactId((String) this.getValueOrNull(ConfigurationKeys.DROOLS_GROUP_ARTIFACT_ID.getKey(), groupConfig));
            g.setVersion((String) this.getValueOrNull(ConfigurationKeys.DROOLS_GROUP_VERSION.getKey(), groupConfig));
            g.setSessionType((String) this.getValueOrNull(ConfigurationKeys.DROOLS_GROUP_SESSION_TYPE.getKey(),
                    groupConfig));

            // TODO Cast Problems using getValueOrNull with Long
            g.setScanFrequency(groupConfig.getLong(ConfigurationKeys.DROOLS_GROUP_SCAN_TIME.getKey()));

            String groupName = (String) this.getValueOrNull(ConfigurationKeys.DROOLS_GROUP_NAME.getKey(), groupConfig);
            g.setName(groupName);

            groups.put(groupName, g);

        }

        return groups;
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

    public DroolsConfigurationBean getDroolsConfiguration() {
        return droolsConfiguration;
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
