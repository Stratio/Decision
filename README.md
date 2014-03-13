StratioBus
==========

The Bus is a key element of the SDS architecture in charge of supporting the life-cycle of streaming data. It is a composition of queues and dispatcher components that temporarily stores streaming data. The META component forwards the data to the BUS in order to be able to perform streaming processing and to optionally persist ephemeral data to an existing table.

Requirements
============

  * Scala 2.10.3
  * sbt 0.13
  * zookeeper
  * kafka 0.8.0

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

 