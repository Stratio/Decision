DECISION-CEP-ENGINE
===================


Decision CEP engine is a Complex Event Processing platform built on Spark Streaming.

It is the result of combining the power of Spark Streaming as a continuous computing framework and Siddhi CEP engine as complex event processing engine.


What is Complex Event Processing?
--------------------------

Complex event processing, or CEP, is event processing that combines data from multiple sources to infer events or patterns that suggest more complicated circumstances.

 CEP as a technique helps discover complex events by analyzing and correlating other events


Decision Cep Engine components
-----------------------------------------

- [Engine](engine/README.md)
- [Api](api/README.md)
- [Shell](shell/README.md)
- [Examples](examples/README.md)
- [Documentation](https://stratio.atlassian.net/wiki/display/PLATFORM/STRATIO+DECISION)



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


* Auditing all the requests in the decision engine (Cassandra or MongoDB)
* Statistics (requests per operation, requests per stream…)
* Failover system (recovering windows, streams and queries from Cassandra or MongoDB)


Decision Cep Engine: API
------------------------------

* Java & Scala API
* Simple programming model
* Available as maven dependency



Decision Cep Engine: SHELL
----------------------------------

* Autocomplete & help
* Tab-completion for stream names
* Built on the API



Interesting facts about Decision Cep Engine
-----------------------------------------------

 * It was presented in Spark Summit 2014 ([link](http://spark-summit.org/2014/talk/stratio-streaming-a-new-approach-to-spark-streaming))
 * Up to 10 million events per minute in a single node.
 * It is fully open source. 


Changelog
---------

See the [changelog](CHANGELOG.md) for changes.

