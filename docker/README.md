## Entrypoint variables

Start the image using the following environment variables (if needed):

###### KAFKA_HOSTS
[IP:PORT] Lists (separated by commas) of the Kafka nodes.
By default: KAFKA_HOSTS=localhost:9092

###### ZOOKEEPER_HOSTS
[IP:PORT] Lists (separated by commas) of the Zookeeper nodes.
By default: ZOOKEEPER_HOSTS=localhost:2181

###### CASSANDRA_HOSTS
[IP:PORT] Lists (separated by commas) of the Cassandra nodes.
By default: CASSANDRA_HOSTS=localhost:9042

###### MONGO_HOSTS
[IP:PORT] Lists (separated by commas) of the Mongo nodes.
By default: MONGO_HOSTS=localhost:27017

###### ES_HOSTS
[IP:PORT] Lists (separated by commas) of the Elasticsearch nodes.
By default: ES_HOSTS=localhost:9300

###### SOLR_HOSTS
[IP:PORT] Lists (separated by commas) of the Solr nodes.
By default: SOLR_HOSTS=localhost:8983

###### FAILOVER_ENABLED
[TRUE or FALSE] To preserve your previous streams and queries configuration

###### CLUSTERING_ENABLED
[TRUE or FALSE] Enable HA.
By default: CLUSTERING_ENABLED=false

###### GROUP_ID
[STRING] To configure different Decision instances as members of the same cluster.
By default: GROUP_ID=default

###### CLUSTER_GROUPS
[STRING] List (separated by commas) of cluster groups.
By default: #clusterGroups = ["groupA", "groupB"]

###### DATATOPICS
[STRING] Lists (separated by commas) of topic to consume from Kafka.
By default: #dataTopics = ["topic_A"]