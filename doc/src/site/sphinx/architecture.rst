Architecture Guide
******************

Stratio Decision is composed of three main elements:

-  A Streaming and CEP engine `Apache Spark Streaming <http://spark.apache.org>`_ and `Siddhi CEP <http://siddhi.sourceforge.net>`_.

-  A Scala API. The API receives the different requests allowing create streams, define queries and so on. The API can be used to create your custom software integrating the Decision capabilities.

-  Decision Shell. A Common Line Interface which allow users interact with Decision engine easily. It expose different commands and help making easy to the users work with Decision. Decision Shell uses the Decision API to interact with the Engine.

-  A publish-subscribe messaging system `Apache Kafka <http://kafka.apache.org/>`_. Decision uses Kafka and Zookeeper as tools to communicate and keep configuration. Decision reads the streaming information from Kafka in real time, and build failover capabilities storing configuration in Zookeeper.

-  Stratio Decision receives the real time information from Kafka. In Stratio usually we use `Stratio Ingestion <https://github.com/Stratio/Ingestion/>`_ to add data to Stratio Decision, because it's a very useful way to get data from many sources, transform it if needed and redirect to Stratio Decision in real-time. If it sounds fine for you, maybe you can take a look at `Stratio Decision Sink <https://github.com/Stratio/Ingestion/tree/master/stratio-sinks/stratio-decision-sink>`_ of Stratio Ingestion project.


 .. image:: images/about-overview.jpg
    :width: 70%
    :align: center





 .. image:: images/decision-sql.jpg
    :width: 70%
    :align: center

Versions
========

Stratio Decision has been tested in the following versions:

-   Cassandra 2.x
-   MongoDB 3.x
-   Kafka 0.8
-   Solr 4.x