.. _quick-reference:

Quick Reference
***************

Configuring the Shell
=====================

Before starting the Shell, review the configuration and make sure it points correctly to your running Kafka brokers
and Zookeeper quorum. On a standard installation, you should find the shell configuration in
/opt/sds/decision-shell/config/shell.properties.


Starting the Shell
==================

Stratio Decision distribution includes a bundled shell for interacting with the engine. For using it, just go to
the installation folder (typically /opt/sds/decision-shell) and run the command::

    ./bin/shell

Using the Shell
===============

Shell management commands
-------------------------

You can get a list of all the available actions by running the command **help**. Also, you can quit the shell at any time
with the command **exit** or **quit**.

The **script** command allows you to load a file with predefined commands. There are other typical commands like **version**
 (for showing the current shell version), **system properties** (displays the shell's properties), **cls** (for clearing the console)
 and **date** (displays the current local time). You can also add commands with **//** or **;** at the start of a line, and execute OS native
commands adding **!** at the start.


Stream management commands
--------------------------

There are several commands for interacting with streams using:

    -**list**: lists all streams (and their associated actions) and queries.

    -**create**: creates a new stream with the given structure (create --stream sensor_grid --definition "name.string,data.double")

    -**drop**: removes the given stream from the engine (drop --stream sensor_grid)

    -**insert**: insert a new event in the given stream (insert --stream sensor_grid --values "name.cpu,data.33")

    -**add query**: adds a query to the specified stream (add query --stream sensor_grid --definition "from sensor_grid#window.length(250) select name, avg(data) as data group by name insert into sensor_grid_avg  for current-events")

    -**remove query**: removes the specified query from the given stream (remove query --stream sensor_grid --id sensor_grid-657c1720-1869-4406-b42a-96b2b8f740b3)

    -**columns**: list all streams querys in the engine.

Stream action commands
----------------------

There are several actions you can perform over available streams (user created or inferred from defined queries):

    -**index start/stop**: starts/stops indexing the stream in the defined Elasticsearch cluster, creating a new index with the stream name.

    -**listen start/stop**: starts/stops sending the stream to the configured Kafka bus, creating a new topic with the stream name.

    -**save cassandra start/stop**: starts/stops saving the stream in Cassandra, under the keyspace **stratio_streaming** and using a table with the stream name.

    -**save mongo start/stop**: starts/stops saving the given stream in MongoDB, using the database **stratio_streaming** and a collection with the stream name.

    -**save solr start/stop**: starts/stops indexing the given stream in Solr, creating and using a core with the stream name.


List of API's
-------------

-   com.stratio.decision
    -   API     custom api in Scala for Stratio Decision
-   com.stratio.decision
    -   Siddhi  Siddhi CEP is a lightweight, easy-to-use Open Source Complex Event Processing Engine (CEP) under Apache Software License v2.0. Siddhi CEP processes events which are triggered by various event sources and notifies appropriate complex events according to the user specified queries.
-   com.datastax.cassandra
    -   cassandra-driver-core   driver of Datastax to connect to Cassandra database
-   org.apache.kafka
    -   kafka_2.10  driver of Apache to connect to Kafka
-   org.mongodb
    -   mongo-java-driver   driver of MongoDB to connect java with a mongoDB database
-   org.apache.solr
    -   solr-solrj  Java client to access solr. It offers a java interface to add, update, and query the solr index.
-   com.codahale.metrics
    -   metrics-core    A Go library which provides light-weight instrumentation for your application.
-   com.ryantenney.metrics
    -   metrics-spring  The metrics-spring module integrates Dropwizard Metrics library with Spring, and provides XML and Java configuration.
-   org.mockito
    -   mockito-all Mockito is a mocking framework for unit tests in Java and has an automated release system
-   junit
    -   junit 4.10  JUnit is a simple framework to write repeatable tests. It is an instance of the xUnit architecture for unit testing frameworks.
