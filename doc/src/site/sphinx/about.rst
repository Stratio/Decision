About Stratio Streaming
***********************

Nowadays data-intensive processes and organizations of all sorts require the use of real-time data
with increasing flexibility and complexity, so we created Stratio Streaming to meet this demand.
Stratio Streaming is one of the core modules on which the Stratio platform is based.
Stratio Streaming is the union of a real-time messaging bus with a complex event processing engine
using Spark Streaming. Thanks to this technology, Stratio Streaming allows the creation of streams
and queries on the fly, sending massive data streams, building complex windows over the data or
manipulating the streams in a simple way by using an SQL-like language.
Stratio Streaming’s API masks the complexity of the system so that developers can work with live
streams straightaway. The engine Stratio Streaming also offers built-in solution patterns which
solve typical use cases and common problems related to streaming. Additionally, we have added global
features to the engine such as auditing and statistics.

Introduction
============

Many big data applications must process batch scenarios combined with
large streams of live data and provide results in near-real-time, but
the lack of an hybrid framework (batch/real) forces the companies to
double the effort to implement new functions.

Existing traditional streaming systems have a
event-driven-record-at-a-time processing model, keeping states by
record. Making stateful streams processing be fault-tolerant is
challenging.

With the advent of Apache Spark, all these issues and challenges have
been resolved and now there is a new and powerful way to design
distributed, scalable and fault-tolerant systems. For that reason,
Stratio Streaming Engine is based on Apache Spark Streaming.

But real-time and big data require a high level of flexibility,
performance, powerful features and on-demand queries, so we have
included Complex Event Processing capabilities and real-time operations.

This way, you can use Stratio Streaming to define or alter your streams
on the fly, launch a complex query with sliding temporal windows, or
save your streams to Apache Cassandra… and these are only a few examples
of the features available.

Features
========

Operations on streams:
----------------------

Streams are conceived are event flows. They have a known definition
including typed fields and they should be use as flowing channels of
events. In other words, streams have an in-flows and output-flows.

You can see streams as SQL tables in a relational database, but streams
are created in an ephemeral environment and no relationship or
constraint rules are applied on them.

**Stratio Streaming lets you *create, alter or drop streams* on the
fly.**

Queries on streams:
-------------------

You can define queries on streams in real-time, including Complex Event
Processing operators, such as:

-  **Filters**: filtering events by the fields.
-  **Windows**: restrict the query to time sliding windows or length
   sliding windows.
-  **Event sequences**: Identifying sequences of events in strict order.
-  **Event patterns:** Identifying events that match a given pattern,
   not necessarily ordered.
-  **Stream union**: Joining streams to identify common events in
   isolated streams.
-  **Projection, group by and built-in functions**: group by, having,
   sum, avg, count operators, among others, are available.

Actions on streams:
-------------------

There are some built-in operations ready to use on streams:

-  Persistence: using Apache Cassandra as long-term storage.
-  Statistics: throughput by operation.
-  Auditing: using Apache Cassandra as long-term storage.

Stream Query Language
---------------------

In order to use all these features, Stratio Streaming has a SQL-like
language called Stream Query Language.

This way, the previous explained features can be organized in a similar
way than SQL:

-  Stream Definition Language (SDL):

   -  Add and remove queries.

   - Create, alter or drop a stream.
-  Stream Manipulation Language (SML):

   -  Insert events into a stream

   - List existing streams in the engine.
-  Stream Action Language (SAL):

   -  Manage listeners on stream (start listen, stop listen).

   - Stream persistence, i.e. write stream to Cassandra.
-  Built-in functions:

   -  Auditing all the request in the streaming engine.
   -  Statistics (request per operation, request per stream …).

Architecture
============

Stratio Streaming is composed of three main elements:

-  A Scala API.
-  A publish-subscribe messaging system `Apache Kafka <http://kafka.apache.org/>`_.
-  A Streaming and CEP engine `Apache Spark Streaming <http://spark.apache.org>`_ and `Siddhi CEP <http://siddhi.sourceforge.net>`_.

 .. image:: images/about-overview.png
    :width: 70%
    :align: center

Where to go from here
=====================

To explore and play with Stratio Streaming, we recommend to visit the
following:

-  :ref:`basic-application`: a step by step tutorial to write an application using Stratio Streaming API.
-  :ref:`using-stratio-streaming-api`: snippets in Java and Scala.
-  :ref:`stratio-streaming-sandbox`
