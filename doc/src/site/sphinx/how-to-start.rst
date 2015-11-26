How do I get started?
*********************

To explore and play with Stratio Decision, we recommend to visit the following:

-  :ref:`basic-application`: a step by step tutorial to write an application using Stratio Decision API.

-  :ref:`apis-custom-transformations`: snippets in Java and Scala.

-  :ref:`stratio-streaming-sandbox`.

-  Read the :ref:`stratio-streaming-sandbox` section to understand how to build your custom queries.


What is Complex Event Processing?
---------------------------------

Complex event processing, or CEP, is event processing that combines data from multiple sources to infer events or patterns that suggest more complicated circumstances.

CEP as a technique helps discover complex events by analyzing and correlating other events.


Decision Cep Engine components
------------------------------

- [Engine] (engine/README.md).
- [Api] (api/README.md).
- [Shell] (shell/README.md).
- [Examples] (examples/README.md).
- [Documentation] (http://docs.stratio.com/modules/decision/development/).



Stream Query Language
---------------------


**1. Stream Definition Language (SDL)**

* Create, alter or drop a stream, add new queries or remove existing queries.


**2. Stream Manipulation Language (SML)**

* Insert events into a stream and list the existing streams in the engine.

**3. Stream Action Language (SAL)**

* Listen to a stream (Kafka), save the stream to Cassandra or MongoDB (auto-creation of tables), index the stream to ElasticSearch or Solr… here you should find some useful operations ready to use.

* Start & Stop each action on-demand.


**4. Built-in functions**

* Auditing all the requests in the decision engine (Cassandra or MongoDB).
* Statistics (requests per operation, requests per stream…).
* Failover system (recovering windows, streams and queries from Zookeeper).


Decision Cep Engine: API
------------------------

* Java & Scala API.
* Simple programming model.
* Available as maven dependency.



Decision Cep Engine: SHELL
--------------------------

* Autocomplete & help.
* Tab-completion for stream names.
* Built on the API.



Interesting facts about Decision Cep Engine
-------------------------------------------

* It was presented in Spark Summit 2014 ([link](http://spark-summit.org/2014/talk/stratio-streaming-a-new-approach-to-spark-streaming))
* Up to 10 million events per minute in a single node.
* It is fully open source.


Decision CEP engine FAQ
-----------------------


**Is Decision CEP engine multi-persistence?**

*For sure, we have included ready-to-use actions in the engine that allows you, any time, to start or stop saving all the events in that stream to the persistence of your choice: MongoDB, Cassandra or ElasticSearch.*

*The engine takes care about creating keyspaces, tables, collections, indexes or whatever it needs to properly store the events (and, what’s more, if the stream is changed by an alter request, Stratio Decision will also change the persistence for you).*

**Can I work with temporal windows?**

*Time is a first-class citizen in a CEP engine so yes, you can work with temporal windows. Anyway, length windows and others are also supported, and there are a lot of operators for your queries (avg, count, sum, max, min, patterns, sequences, joins…).*

**How can I send data to the engine?**

*Use the API or the Shell provided by Decision CEP engine. You can send a really BIG amount of events.*



System Requirements
===================

Stratio Decision needs Java 7, a Zookeeper instance and a Kafka instance installed to work properly.

As a developer you need basic knowledge about Spark Streaming.