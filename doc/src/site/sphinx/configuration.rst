Configuration
*************

Configuring the Engine
======================

Before starting the Stratio Decision Engine, you must review the configuration to adapt it to Kafka and Zookeeper. On a standard installation, you should find the configuration file in /opt/sds/decision/config

Print Streams Configuration
---------------------------
**This field is mandatory**

You can set the "printStreams" value to true or false to print streams in each spark iteration.

Example::

    printStreams = false

Stats Configuration
-------------------
**This field is mandatory**

You can enable the stats by setting the value of "statsEnabled" to true/false.

Example::

    statsEnabled = false

Audit Configuration
-------------------
**This field is mandatory**

It is possible to save all actions created on the platform setting the "auditEnabled" value to true (or disable it with false)

Example::

    auditEnabled = false

Failover Configuration
----------------------
**This field is mandatory**

You can enable the failover for saving the engine state in Zookeeper. You must have configured the Zookeeper settings and the enabled field to true. If Stratio Decision closes abruptly when you start it again, it will recover its last state from Zookeeper.

Example::

    failover = {
        enabled = false
        period = 300 s
    }

Kafka Configuration
-------------------
**This field is mandatory**

You must configure the Kafka configuration setting the value of your Kafka hosts with a list of IP and port. You can modify the values of connectionTimeout, sessionTimeout, replicationFactor or partitions, as well.

Example::

    kafka = {
        hosts = ["localhost:9092"]
        connectionTimeout = 10000
        sessionTimeout = 10000

        # default replication factor and partitions for internal topics
        replicationFactor = 1
        partitions = 1
    }

Zookeeper Configuration
-----------------------
**This field is mandatory**

You have to configure the Zookeeper Quorum by setting the value of your Zookeepr hosts with a list of IP and port.

Example::

    zookeeper = {
        hosts = ["localhost:2181"]
    }

Spark Configuration
-------------------
**This field is mandatory**

You can modify the number of internal hosts of spark and the time of batch.

Example::

    spark = {
        internalHost = "local[6]"
        internalStreamingBatchTime = 2 s

        host ="local[6]"
        streamingBatchTime = 2 s
    }

Cassandra Configuration
-----------------------

You can set the cassandra configuration by setting the hosts of cassandra with IP.

Example::

    cassandra = {
        hosts = ["localhost"]
    }

MongoDB Configuration
---------------------
If you want to save any stream in MongoDB, you must configure in "hosts" the IPs and ports of the nodes and username and password if it is necessary.

Example::

    mongo = {
        hosts = ["localhost:27017"]
        #username = ""
        #password= ""
    }

ElasticSearch Configuration
---------------------------
If you want to save any stream in ElasticSearch, you must configure in "hosts" the IPs and ports of the nodes and the clusterName.

Example::

    elasticsearch = {
        hosts = ["localhost:9300"]
        clusterName = "elasticsearch"
    }

Solr Configuration
------------------
If you want to save any stream in Solr, you must configure its settings selecting true or false in "cloud" depending on your installation (Solr Cloud or Standalone). If your installation is in cloud you must indicate in "hosts" the Zookeeper connection, if your installation is standalone you must indicate in "hosts" the IP and port of your machine. In dataDir you have to set the path where Solr will save the index data (make sure that you have read/write permissions)

Example::

    solr = {
        hosts = "localhost:2181"
        cloud = true
        dataDir = "/opt/sds/solr/examples/solr"
    }

Starting the Engine
===================

On a standard installation you can start Stratio Decision Engine by executing the command:

::

    ./opt/sds/decision/bin/run
