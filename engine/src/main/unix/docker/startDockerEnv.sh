#!/bin/bash
#
# Copyright (C) 2014 Stratio (http://stratio.com)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


VERSION_ZOOKEEPER="3.3.6"
VERSION_KAFKA="0.8.1.1"
VERSION_MONGO="3.0.4"
VERSION_ELASTIC="2.1.8"
VERSION_CASSANDRA="1.4.2"

isUpDockerImage() {
    dockerImage=$1
    IS_UP_IMAGE=`docker ps|grep $1|wc -l`
    return "$IS_UP_IMAGE"
}

existsDockerImage() {
    dockerImage=$1
    EXISTS_IMAGE=`docker images|grep $1|wc -l`
    return "$EXISTS_IMAGE"
}


## Cheking if it's the first time we run the auxiliary containers

EXISTS_SKYDNS=`docker images|grep skydns|wc -l`

if [ "$EXISTS_SKYDNS" -eq "1" ]; then
    echo "Skydns Image already exists!"
    IS_UP_SKYDNS=`docker ps|grep skydns|wc -l`

    if [ "$IS_UP_SKYDNS" -eq "1" ]; then
        echo "Skydns Image is already up"
    else
        echo "Starting Skydns Image ..."
        docker run -d -p 172.17.42.1:53:53/udp --restart=always -i crosbymichael/skydns -nameserver 8.8.8.8:53 -domain dev
    fi
else
    echo "Skydns Image doesn't exists! Trying to start Skydns"
    docker run -d -p 172.17.42.1:53:53/udp --restart=always --name skydns crosbymichael/skydns -nameserver 8.8.8.8:53 -domain dev
fi

docker ps|grep skydns

EXISTS_SKYDOC=`docker images|grep skydoc|wc -l`
if [ "$EXISTS_SKYDOC" -eq "1" ]; then
    echo "Skydoc Image already exists!"
    IS_UP_SKYDOC=`docker ps|grep skydoc|wc -l`

    if [ "$IS_UP_SKYDOC" -eq "1" ]; then
        echo "Skydoc Image is already up"
    else
        echo "Starting Skydoc Image ..."
        docker run -d -v /var/run/docker.sock:/docker.sock --restart=always -i crosbymichael/skydock -ttl 30 -environment local -s /docker.sock -domain dev -name skydoc
    fi
else
    echo "Skydoc Image doesn't exists! Trying to start Skydoc"
    docker run -d -v /var/run/docker.sock:/docker.sock --restart=always --name skydock crosbymichael/skydock -ttl 30 -environment local -s /docker.sock -domain dev -name skydoc
fi

docker ps|grep skydoc


############################################################
echo "Starting component Docker instances ....."

## ZOOKEEPER ######

isUpDockerImage zookeeper
IS_UP_ZOOKEEPER=$?
existsDockerImage zookeeper
EXISTS_ZOOKEEPER=$?

if [ "$EXISTS_ZOOKEEPER" -eq "1" ]; then
    echo "Zookeeper Image already exists!"
    if [ "$IS_UP_ZOOKEEPER" -ne "0" ]; then
        echo "Zookeeper is already up"
    else
        echo "Starting Zookeeper"
        docker start zk
    fi
else
    docker run -dit --name zk --hostname zk.zookeeper.local.dev qa.stratio.com:5000/zookeeper:$VERSION_ZOOKEEPER
fi


## KAFKA ######

isUpDockerImage kafka
IS_UP_KAFKA=$?
existsDockerImage kafka
EXISTS_KAFKA=$?

if [ "$EXISTS_KAFKA" -eq "1" ]; then
    if [ "$IS_UP_KAFKA" -ne "0" ]; then
        echo "Kafka is already up"
    else
        echo "Starting Kafka"
        docker start kf
    fi
else
    docker run -dit --name kf --hostname kf.kafka.local.dev --env "ZK_CONNECT=zk.zookeeper.local.dev" qa.stratio.com:5000/kafka:$VERSION_KAFKA
fi


## MONGO ######

isUpDockerImage mongo
IS_UP_MONGO=$?
existsDockerImage mongo
EXISTS_MONGO=$?

if [ "$EXISTS_MONGO" -ne "0" ]; then
    if [ "$IS_UP_MONGO" -ne "0" ]; then
        echo "Mongo is already up"
    else
        echo "Starting Mongo"
        docker start mongo
    fi
else
    docker run -dit --name mongo --hostname db.mongo.local.dev qa.stratio.com:5000/stratio/mongo:$VERSION_MONGO
fi


## CASSANDRA ######

isUpDockerImage cassandra
IS_UP_CS=$?
existsDockerImage cassandra
EXISTS_CS=$?

if [ "$EXISTS_CS" -eq "1" ]; then
    if [ "$IS_UP_CS" -ne "0" ]; then
        echo "Cassandra is already up"
    else
        echo "Starting Cassandra"
        docker start cs
    fi
else
    docker run -dit --name cs --hostname cs.cassandra.local.dev stratio/cassandra:$VERSION_CASSANDRA
fi

## ELASTIC SEARCH ######

isUpDockerImage elasticsearch
IS_UP_ES=$?
existsDockerImage elasticsearch
EXISTS_ES=$?

if [ "$EXISTS_ES" -eq "1" ]; then
    if [ "$IS_UP_ES" -ne "0" ]; then
        echo "ElasticSearch is already up"
    else
        echo "Starting ElasticSearch"
        docker start es
    fi
else
    docker run -dit --name es --hostname es.elastic.local.dev elasticsearch:$VERSION_ELASTIC
fi

docker ps
