Feature Guide
*************

Operations on streams:
----------------------

Streams are conceived are event flows. They have a known definition
including typed fields and they should be use as flowing channels of
events. In other words, streams have an in-flows and output-flows.

You can see streams as SQL tables in a relational database, but streams
are created in an ephemeral environment and no relationship or
constraint rules are applied on them.

**Stratio Decision lets you *create, alter or drop streams* on the
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

In order to use all these features, Stratio Decision has a SQL-like
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

   -  Auditing all the request in the Decision engine.
   -  Statistics (request per operation, request per stream ???).