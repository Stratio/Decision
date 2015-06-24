STREAMING-CEP-ENGINE
===================


Streaming CEP engine is a Complex Event Processing platform built on Spark Streaming.

It is the result of combining the power of Spark Streaming as a continuous computing framework and Siddhi CEP engine as complex event processing engine.


What is Complex Event Processing?
--------------------------

Complex event processing, or CEP, is event processing that combines data from multiple sources to infer events or patterns that suggest more complicated circumstances.

 CEP as a technique helps discover complex events by analyzing and correlating other events


Streaming Cep Engine components
-----------------------------------------

- [Engine](engine/README.md)
- [Api](api/README.md)
- [Shell](shell/README.md)
- [Examples](examples/README.md)
- [Documentation](http://docs.stratio.com/modules/streaming-cep-engine/development/)



Stream Query Language
----------------------------


**1 Stream Definition Language (SDL)**

* Create, alter or drop a stream, add new queries or remove existing queries


**2 Stream Manipulation Language (SML)**

* Insert events into a stream and list the existing streams in the engine.

**3 Stream Action Language (SAL)**

* Listen to a stream (kafka), save the stream to Cassandra or mongoDB (auto-creation of tables), index the stream to ElasticSearch or Solr… here you should find useful operations ready to use.

* Start & Stop each action on-demand


**4 Built-in functions**


* Auditing all the requests in the streaming engine (Cassandra or MongoDB)
* Statistics (requests per operation, requests per stream…)
* Failover system (recovering windows, streams and queries from Cassandra or MongoDB)


Streaming Cep Engine: API
------------------------------

* Java & Scala API
* Simple programming model
* Available as maven dependency



Streaming Cep Engine: SHELL
----------------------------------

* Autocomplete & help
* Tab-completion for stream names
* Built on the API



Interesting facts about Streaming Cep Engine
-----------------------------------------------

 * It was presented in Spark Summit 2014 ([link](http://spark-summit.org/2014/talk/stratio-streaming-a-new-approach-to-spark-streaming))
 * Up to 10 million events per minute in a single node.
 * It is fully open source. 


Streaming CEP engine FAQ
-------------------------


**Is Streaming CEP engine multi-persistence?**

*For sure, we have included ready-to-use actions in the engine that allows you, any time, to start or stop saving all the events in that stream to the persistence of your choice: MongoDB, Cassandra or ElasticSearch.*

*The engine takes care about creating keyspaces, tables, collections, indexes or whatever it needs to properly store the events (and,what’s more, if the stream is changed by an alter request, Stratio Streaming will also change the persistence for you).*

**Can I work with temporal windows?**

*Time is a first-class citizen in a CEP engine so yes, you can work with temporal windows. Anyway, length windows and others are also supported, and there are a lot of operators for your queries (avg, count, sum, max, min, patterns, sequences, joins…)*

**How can I send data to the engine?**

*Use the API or the Shell provided by Streaming CEP engine. You can send a really BIG amount of events.*


Changelog
---------

See the [changelog](CHANGELOG.md) for changes.

