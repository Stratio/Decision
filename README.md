Stratio Streaming API
=====================

The Stratio Streaming API provides a simple interface to the Stratio Streaming Engine component.

Architecture
============

This component forwards the incoming requests to the Stratio Streaming Engine component via kafka topics.

Upon receiving a message the component will send a KeyedMessage to a kafka topic for which the key will be the message operation (create, select, insert...). The Stratio streaming engine will be listening to these topics and handles the messages accordingly to their type.

The comunication between the Bus and Stratio streaming components is managed by acknowledges sending via zookeeper. There are two operation types:

   * Sync operations: Once the Bus sends a message to the kafka topic it will be waiting for the response reading a zookeeper zNode. If the Streaming component is able to handle the message it will write a response within the zookeeper zNode, if not a timeout expires and the API will throw an Exception.

   * Async operations: The Bus sends a message to the kafka topic and it will not wait for the response.


Requirements
============

  * Scala 2.10.3
  * sbt 0.13
  * zookeeper
  * kafka 0.8.1

How to start
============

  * Clone the project

  * If you are using IntelliJ as your IDE you can import the project by running sbt gen-idea within the parent folder of the project. 

  * To compile the project:
        sbt compile

  * To run the tests (Stratio Streaming Engine should be up and running):
        sbt test

  * To create the Stratio Streaming API distribution:
        sbt assembly

  * To publish the artifact to a nexus repository you need to create a credentials file within the following directory: $USER_HOME/.ivy2.
    The content of the file should be the following:

     realm=Sonatype Nexus Repository Manager
     host={yournexushost}
     user={theuser}
     password={thepassword}

     To publish the artifact just type:

       sbt publish

Using the Stratio Streaming API
===============================

  * Firstly, you need to include in your project CLASSPATH a file called stratio-streaming.conf with the following content:

    kafka.server={kafkaServerAddress}
    kafka.port={kafkaServerPort}
    zookeeper.server={zookeeperServerAddress}
    zookeeper.port={zookeeperServerPort}

  * To get an instance of the Stratio Streaming API:

    val stratioStreamingAPI = StratioStreamingAPIFactory.create().initialize()
 