Stratio Decision API
=====================

The Stratio Decision API provides a simple interface to the Stratio Decision Engine component.

Architecture
============

This component forwards the incoming requests to the Stratio Decision Engine component via kafka topics.

Upon receiving a message the component will send a KeyedMessage to a kafka topic for which the key will be the message operation (create, select, insert...). The Stratio Decision engine will be listening to these topics and handles the messages accordingly to their type.

The comunication between the Bus and Stratio Decision components is managed by acknowledges sending via zookeeper. There are two operation types:

   * Sync operations: Once the Bus sends a message to the kafka topic it will be waiting for the response reading a zookeeper zNode. If the Decision component is able to handle the message it will write a response within the zookeeper zNode, if not a timeout expires and the API will throw an Exception.

   * Async operations: The Bus sends a message to the kafka topic and it will not wait for the response.


Requirements
============

  * Scala 2.10.3
  * sbt 0.13
  * zookeeper
  * kafka 0.8.2.1

How to start
============

  * Clone the project

  * If you are using IntelliJ as your IDE you can import the project by running sbt gen-idea within the parent folder of the project. 

  * To compile the project:
        sbt compile

  * To run the unit tests:
        sbt test

  * To run the integration tests locally (Stratio Decision Engine should be up and running):
        sbt integration:test

  * To run the integration tests pointing to an external server:
        sbt integration:testOnly -- -DzookeeperHost={yourZookeeperHost} -DzookeeperPort={yourZookeeperPort} -DkafkaHost={yourKafkaHost} -DkafkaPort={yourKafkaPort} -DelasticSearchHost=node.stratio.com -DelasticSearchPort=9200 -DcassandraHost=node.stratio.com

  * To create the Stratio Decision API distribution:
        sbt assembly

  * To publish the artifact to a nexus repository you need to create a credentials file within the following directory: $USER_HOME/.ivy2.
    The content of the file should be the following:

     ```
     realm=Sonatype Nexus Repository Manager
     host={yournexushost}
     user={theuser}
     password={thepassword}
     ```

     To publish the artifact just type:

       sbt publish

Using the Stratio Decision API
===============================

  * You can get a Stratio Decision API instance with the following options:

    1º. Call the initializeWithServerConfig method with the Decision Server params:

        ```
        val stratioStreamingAPI = StratioStreamingAPIFactory.create().initializeWithServerConfig(kafkaHost,
                                                                    kafkaPort,
                                                                    zookeeperHost,
                                                                    zookeeperPort)
        ```

    2º. Include in your project CLASSPATH a file called stratio-decision.conf with the following content:

        ```
        kafka.server={kafkaServerAddress}
        kafka.port={kafkaServerPort}
        zookeeper.server={zookeeperServerAddress}
        zookeeper.port={zookeeperServerPort}
        ```
     And then call the initialize method:

     val stratioStreamingAPI = StratioStreamingAPIFactory.create().initialize()
 
