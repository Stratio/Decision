StratioBus
==========

The Bus is a key element of the SDS architecture in charge of supporting the life-cycle of streaming data. It is a composition of queues and dispatcher components that temporarily stores streaming data. The META component forwards the data to the BUS in order to be able to perform streaming processing and to optionally persist ephemeral data to an existing table.

Architecture
============

This component forwards the incoming requests to the StratioStreaming component via kafka topics.

Upon receiving a message the component will send a KeyedMessage to a kafka topic for which the key will be the message operation (create, select, insert...). The Stratio streaming component will be listening to this topic and handle the message accordingly to its type. 

The comunication between the Bus and Stratio streaming components is managed by acknowledges sending via zookeeper. There are two operation types:

   * Sync operations: Once the Bus sends a message to the kafka topic it will be waiting for the response reading a zookeeper zNode. If the Streaming component is able to handle the message it will write a response within the zookeeper zNode, if not a timeout expires and the Bus will throw an Exception.

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

  * To run the tests:
        ./start-test-env.sh 
        sbt test

        //TODO: Create the stop-test-env script

  * To create the StratioBus distribution:
        sbt assembly

Using the Bus
=============

  * To get an instance of StratioBus:

    val stratioBus = StratioBusFactory.create().initialize()
 