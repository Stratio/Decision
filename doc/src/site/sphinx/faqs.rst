Decision FAQ
************

**Is Decision CEP engine multi-persistence?**

For sure, we have included ready-to-use actions in the engine that allows you, any time, to start or stop saving all
the events in that stream to the persistence of your choice: MongoDB, Cassandra, Solr or ElasticSearch.

The engine takes care about creating keyspaces, tables, collections, indexes or whatever it needs to properly store the events (and,what’s more, if the stream is changed by an alter request, Stratio Decision will also change the persistence for you).

**Can I work with temporal windows?**

Time is a first-class citizen in a CEP engine so yes, you can work with temporal windows. Anyway, length windows and others are also supported, and there are a lot of operators for your queries (avg, count, sum, max, min, patterns, sequences, joins…)

**How can I send data to the engine?**

Use the API or the Shell provided by Decision CEP engine. You can send a really BIG amount of events.

**What number of events can Decision CEP process?**

Decision CEP can process up to 10 million events per minute in a single node.

