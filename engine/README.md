
Stratio Streaming {#welcome}
=======


In a nutshell, Stratio Streaming is a real-time engine with Complex Event Processing capabilities with a built-in and powerful Streaming Query Language for doing on-demand operations on your streams.

Stratio streaming provides a simple Java/Scala API to send commands to the engine and retrieve information from existing streams.

The key components of Streaming Engine provide a fault-tolerant, high availability and extremely performant solution in order to work with thousands of miles of events per minute.



:arrow_forward: Running Stratio Streaming Engine


**Usage**

```
--sparkMaster local --zookeeper-cluster fqdn:port,fqdn2:port --kafka-cluster fqdn:port,fqdn2:port --cassandra-cluster fqdn,fqdn2
```


**Using the provided script**

```
sudo sh run-stratio-streaming com.stratio.streaming.StreamingEngine --spark-master local_2 --zookeeper-cluster fqdn:port,fqdn2:port,... --kafka-cluster fqdn:port,fqdn2:port,... --cassandra-cluster fqdn,fqdn,...
```

How to run engine Integration Tests
----------------------------------------------

Engine integration tests use Docker containers to validate the proper integration with Kafka, MongoDB and so on. 
To run the integration tests it's necessary to boot some docker instances in your local environment and set some JVM parameters to connect to those Docker instances.

- Boot auxiliary containers (first time):
```
docker run -d -p 172.17.42.1:53:53/udp --restart=always --name skydns crosbymichael/skydns -nameserver 8.8.8.8:53 -domain dev 
docker run -d -v /var/run/docker.sock:/docker.sock --restart=always --name skydock crosbymichael/skydock -ttl 30 -environment local -s /docker.sock -domain dev -name skydoc
```

- Start existing auxiliary containers:
```
docker run -d -p 172.17.42.1:53:53/udp --restart=always -i crosbymichael/skydns -nameserver 8.8.8.8:53 -domain dev 
docker run -d -v /var/run/docker.sock:/docker.sock --restart=always -i crosbymichael/skydock -ttl 30 -environment local -s /docker.sock -domain dev -name skydoc
```

- Start Zookeeper, Kafka and MongoDB containers:
```
docker run -dit --name zk --hostname zk.zookeeper.local.dev qa.stratio.com:5000/zookeeper:3.3.6
docker run -dit --name kf --hostname kf.kafka.local.dev --env "ZK_CONNECT=zk.zookeeper.local.dev" qa.stratio.com:5000/kafka:0.8.1.1
docker run -dit --name mongo --hostname db.mongo.local.dev qa.stratio.com:5000/stratio/mongo:3.0.4
docker run -dit --name cs --hostname cs.cassandra.local.dev stratio/cassandra:2.1.8
docker run -dit --name es --hostname es.elastic.local.dev elasticsearch:1.4.2
```

- Execute the integration tests adding the following JVM parameters:

    -Dkafka.hosts.0=kf.kafka.local.dev:9092  
    -Dmongo.hosts.0=db.mongo.local.dev:27017  
    -Dzookeeper.hosts.0=zk.zookeeper.local.dev:2181
    -Dcassandra.hosts.0=cs.cassandra.local.dev 
    -Delasticsearch.hosts.0=es.elastic.local.dev:9300






