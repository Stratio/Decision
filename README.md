

Stratio Streaming {#welcome}
=================

In a nutshell, Stratio Streaming is a real-time engine with Complex Event Processing capabilities with a built-in and powerful Streaming Query Language for doing on-demand operations on your streams.

Stratio streaming provides a simple Java/Scala API to send commands to the engine and retrieve information from existing streams.

The key components of Streaming Engine provide a fault-tolerant, high availability and extremely performant solution in order to work with thousands of miles of events per minute.






:arrow_forward: Running Stratio Streaming Engine


sudo sh /path_to_spark/spark-0.9.0-incubating/bin/run-mine com.stratio.streaming.StreamingEngine --spark-master local_2 --zookeeper-cluster node.stratio.com:2181 --kafka-cluster node.strao.com:9092