#!/usr/bin/env bash

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


bootSkyDNS()    {

    EXISTS_SKYDNS=`docker images|grep skydns|wc -l`

    if [ "$EXISTS_SKYDNS" -eq "1" ]; then
        echo "Skydns Image already exists!"
        IS_UP_SKYDNS=`docker ps|grep skydns|wc -l`

        if [ "$IS_UP_SKYDNS" -eq "1" ]; then
            echo "Skydns Image is already up"
        else
            echo "Starting Skydns Image ..."
            docker run -d -p 172.17.42.1:53:53/udp --restart=always --name skydns crosbymichael/skydns -nameserver 8.8.8.8:53 -domain dev
        fi
    else
        echo "Skydns Image doesn't exists! Trying to start Skydns"
        docker run -d -p 172.17.42.1:53:53/udp --restart=always --name skydns crosbymichael/skydns -nameserver 8.8.8.8:53 -domain dev
    fi

    docker ps|grep skydns
}

bootskyDock() {

    EXISTS_SKYDOCK=`docker images|grep skydock|wc -l`
    if [ "$EXISTS_SKYDOCK" -eq "1" ]; then
        echo "Skydock Image already exists!"
        IS_UP_SKYDOCK=`docker ps|grep skydock|wc -l`

        if [ "$IS_UP_SKYDOCK" -eq "1" ]; then
            echo "Skydock Image is already up"
        else
            echo "Starting Skydock Image ..."
            docker run -d -v /var/run/docker.sock:/docker.sock --restart=always --name skydock crosbymichael/skydock -ttl 30 -environment local -s /docker.sock -domain dev -name skydns
        fi
    else
        echo "Skydoc Image doesn't exists! Trying to start Skydock"
        docker run -d -v /var/run/docker.sock:/docker.sock --restart=always --name skydock crosbymichael/skydock -ttl 30 -environment local -s /docker.sock -domain dev -name skydns
    fi

    docker ps|grep skydock
}




############################################################
echo "Starting component Docker instances ....."


## ZOOKEEPER ######
bootZookeeper() {


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
}

## KAFKA ######

bootKafka() {

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
}


## MONGO ######

bootMongo() {

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
        docker run -dit --name mongo --hostname mongo.mongo.local.dev qa.stratio.com:5000/stratio/mongo:$VERSION_MONGO
    fi
}

## CASSANDRA ######

bootCassandra() {
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
}

## ELASTIC SEARCH ######

bootElastic() {
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
}

bootSkyDNS
bootskyDock
bootZookeeper
bootKafka
bootMongo
bootElastic
bootCassandra

docker ps
