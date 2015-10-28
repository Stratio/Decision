Production Checklist & Trouble Shooting Guide
*********************************************

Production Checklist
====================

The best way to deploy Stratio Decision in live environment includes the following points:

-   Install and start a **Zookeeper** cluster

-   Install and start a **Kafka** cluster

-   Install Stratio Decision service and :ref:`configuration` Zookeeper and Kafka

-   Install Stratio Decision Shell and configure Zookeeper and Kafka


After to configure all the elements check the following points:

-   Zookeeper is up and running, check that zookeeper process is running in all cluster nodes and Zookeeper port is in use (usually the 2181 port)

-   Kafka is up and running, check that kafka process is running in all cluster nodes and Kafka port is in use (usually the 9092 port)

-   Check that Stratio Decision is up and running

-   Check that connectivity between different cluster nodes in selected ports is opened

-   Execute the Stratio Decision Shell and execute the **list** command. If you don't see any error, the shell is connecting properly


Trouble Shooting
================

Those are the most common errors and the way to solve it:

* Stratio Decision Engine is not starting properly. Check that all ports are opened. Is important to start all the components in order, first of all Zookeeper, secondly Kafka and finally Decision engine and shell. Stop and start in the proper order.

* Stratio Decision or Shell are not connecting properly to Kafka. In the properties file of different elements check that you are using the FQDN hostname always in the same way.

* I can't save from Decision to Solr, Cassandra, Elastic Search or MongoDB. Check that connectivity between Decision and datastores are opened. Check also the user & password used in Decision properties file.

* Decision Shell is not saving my queries. If you can execute a **list** command and the Shell is returning a Query  syntax error review your CEP query. Take a look at :ref:`cep-query-syntax` section

* When I restart Decision all my existing configuration doesn't exists and I've to apply it again. Enable the failover configuration. Take a look at :ref:`configuration` section
