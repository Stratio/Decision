#!/bin/bash -xe
 sed -i "s|localhost:9092|${KAFKA_HOSTS:=localhost:9092}|" /etc/sds/decision/config.conf
 sed -i "s|localhost:2181|${ZOOKEEPER_HOSTS:=localhost:2181}|" /etc/sds/decision/config.conf
 sed -i "s|localhost:9042|${CASSANDRA_HOSTS:=localhost:9042}|" /etc/sds/decision/config.conf
 sed -i "s|localhost:27017|${MONGODB_HOSTS:=localhost:27017}|" /etc/sds/decision/config.conf
 sed -i "s|localhost:9300|${ES_HOSTS:=localhost:9300}|" /etc/sds/decision/config.conf
 sed -i "s|localhost:8983|${SOLR_HOSTS:=localhost:8983}|" /etc/sds/decision/config.conf
 sed -i "s|failoverEnabled.*|failoverEnabled = ${FAILOVER_ENABLED:=false}|" /etc/sds/decision/config.conf
 sed -i "s|partitionsEnabled.*|partitionsEnabled = ${PARTITIONS_ENABLED:=false}|" /etc/sds/decision/config.conf
 sed -i "s|enabled.*|enabled = ${CLUSTERING_ENABLED:=false}|" /etc/sds/decision/config.conf
 sed -i "s|groupId.*|groupId = \"${GROUP_ID:=default}\"|" /etc/sds/decision/config.conf
 if [ "x${CLUSTER_GROUPS}x" != "xx" ]; then
   CG=""
   for G in $CLUSTER_GROUPS; do
      CG="$G",$CG
   done
   CG=\[${CG::-1}\]
   sed -i "s|#clusterGroups.*|clusterGroups = ${CG}|" /etc/sds/decision/config.conf
 fi
 if [ "x${DATATOPICS}x" != "xx" ]; then
   DT=""
   for T in $DATATOPICS; do
      DT="$T",$DT
   done
   DT=\[${DT::-1}\]
   sed -i "s|#dataTopics.*|dataTopics = ${DT}|" /etc/sds/decision/config.conf
 fi
 /etc/init.d/decision start
 tail -F /var/log/sds/decision/decision.log