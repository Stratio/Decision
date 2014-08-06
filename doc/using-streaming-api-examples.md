---
title: Using Stratio Streaming API (examples)
---

Table of Contents
=================

-   [Creating and initializing the API](#creating-and-initializing-the-api)
-   [Creating and initializing the API with server configuration](#creating-and-initializing-the-api-with-server-configuration)
-   [Creating a new stream](#creating-a-new-stream)
-   [Adding columns to an existing stream](#adding-columns-to-an-existing-stream)
-   [Inserting data into a stream](#inserting-data-into-a-stream)
-   [Adding queries to streams](#adding-queries-to-streams)
-   [Removing an existing stream](#removing-an-existing-stream)
-   [Removing an existing query from a stream](#removing-an-existing-query-from-a-stream)
-   [Listening to streams](#listening-to-streams)
-   [Stop listening to streams](#stop-listening-to-streams)
-   [Save the stream to cassandra](#save-the-stream-to-cassandra)
-   [Stop saving the stream to cassandra](#stop-saving-the-stream-to-cassandra)
-   [Save the stream to MongoDB](#save-the-stream-to-mongodb)
-   [Stop saving the stream to MongoDB](#stop-saving-the-stream-to-mongodb)
-   [Index the stream to elasticsearch](#index-the-stream-to-elasticsearch)
-   [Stop indexing the stream to elasticsearch](#stop-indexing-the-stream-to-elasticsearch)
-   [Getting the list of all the streams and their queries](#getting-the-list-of-all-the-streams-and-their-queries)

Creating and initializing the API
=================================

{% tabgroup %}
{% tab Scala %}
~~~~ {prettyprint lang-java}
val stratioStreamingAPI = StratioStreamingAPIFactory.create().initialize
~~~~
{% endtab %}
{% tab Java %}
~~~~ {prettyprint lang-java}
IStratioStreamingAPI stratioStreamingAPI = StratioStreamingAPIFactory.create().initialize();
~~~~
{% endtab %}
{% endtabgroup %}

Creating and initializing the API with server configuration
===========================================================

{% tabgroup %}
{% tab Scala %}
~~~~ {prettyprint lang-java}
val stratioStreamingAPI = StratioStreamingAPIFactory.create().initializeWithServerConfig("stratio.node.com", 9092, "stratio.node.com", 2181)
~~~~
{% endtab %}
{% tab Java %}
~~~~ {prettyprint lang-java}
IStratioStreamingAPI stratioStreamingAPI = StratioStreamingAPIFactory.create().initializeWithServerConfig("stratio.node.com", 9092, "stratio.node.com", 2181);
~~~~
{% endtab %}
{% endtabgroup %}

Creating a new stream
=====================

{% tabgroup %}
{% tab Scala %}
~~~~ {prettyprint lang-java}
val firstStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
val secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING)
val streamName = "testStream"
val columnList = Seq(firstStreamColumn, secondStreamColumn)
try {
   stratioStreamingAPI.createStream(streamName, columnList)
} catch {
   case ssEx: StratioStreamingException => println(ssEx.printStackTrace())
}
~~~~
{% endtab %}
{% tab Java %}
~~~~ {prettyprint lang-java}
ColumnNameType firstStreamColumn= new ColumnNameType("column1", ColumnType.INTEGER);
ColumnNameType secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING);
String streamName = "testStream";
List columnList = Arrays.asList(firstStreamColumn, secondStreamColumn);
try {
     stratioStreamingAPI.createStream(streamName, columnList);
} catch (StratioStreamingException e) {
     e.printStackTrace();
}
~~~~
{% endtab %}
{% endtabgroup %}

Adding columns to an existing stream
====================================

{% tabgroup %}
{% tab Scala %}
~~~~ {prettyprint lang-java}
val newStreamColumn = new ColumnNameType("column3", ColumnType.DOUBLE)
val streamName = "testStream"
val columnList = Seq(newStreamColumn)
try {
      stratioStreamingAPI.alterStream(streamName, columnList)
} catch {
      case ssEx: StratioStreamingException => println(ssEx.printStackTrace())
}
~~~~
{% endtab %}
{% tab Java %}
~~~~ {prettyprint lang-java}
ColumnNameType thirdStreamColumn= new ColumnNameType("column3", ColumnType.DOUBLE);
String streamName = "testStream";
List columnList = Arrays.asList(thirdStreamColumn);
try {
    stratioStreamingAPI.alterStream(streamName, columnList);
} catch (StratioStreamingException e) {
    e.printStackTrace();
}
~~~~
{% endtab %}
{% endtabgroup %}

Inserting data into a stream
============================

{% tabgroup %}
{% tab Scala %}
~~~~ {prettyprint lang-java}
val streamName = "testStream"
val firstColumnValue = new ColumnNameValue("column1", new Integer(1))
val secondColumnValue = new ColumnNameValue("column2", "testValue")
val thirdColumnValue = new ColumnNameValue("column3", new Double(2.0))
val streamData = Seq(firstColumnValue, secondColumnValue, thirdColumnValue)
try {
    stratioStreamingAPI.insertData(streamName, streamData)
} catch {
   case ssEx: StratioStreamingException => println(ssEx.printStackTrace())
}
~~~~
{% endtab %}
{% tab Java %}
~~~~ {prettyprint lang-java}
String streamName = "testStream";
ColumnNameValue firstColumnValue = new ColumnNameValue("column1", new Integer(1));
ColumnNameValue secondColumnValue = new ColumnNameValue("column2", "testValue");
ColumnNameValue thirdColumnValue = new ColumnNameValue("column3", new Double(2.0));
List<ColumnNameValue> streamData = Arrays.asList(firstColumnValue, secondColumnValue, thirdColumnValue);
try {
      stratioStreamingAPI.insertData(streamName, streamData);
} catch(StratioStreamingException ssEx) {
    ssEx.printStackTrace();
}
~~~~
{% endtab %}
{% endtabgroup %}

Adding queries to streams
=========================

{% tabgroup %}
{% tab Scala %}
~~~~ {prettyprint lang-java}
val query = "from testStream select column1, column2, column3 insert into alarms for current-events"
val streamName = "testStream"
try {
  val queryId = stratioStreamingAPI.addQuery(streamName, query)
} catch {
  case ssEx: StratioStreamingException => println(ssEx.printStackTrace())
}
~~~~
{% endtab %}
{% tab Java %}
~~~~ {prettyprint lang-java}
String streamName = "testStream";
String query = "from testStream select column1, column2, column3 insert into alarms for current-events";
try {
   String queryId = stratioStreamingAPI.addQuery(streamName, query);
} catch(StratioStreamingException ssEx) {
   ssEx.printStackTrace();
}
~~~~
{% endtab %}
{% endtabgroup %}

Removing an existing stream
===========================

{% tabgroup %}
{% tab Scala %}
~~~~ {prettyprint lang-java}
val streamName = "testStream"
try {
    stratioStreamingAPI.dropStream(streamName)
} catch {
   case ssEx: StratioStreamingException => println(ssEx.printStackTrace())
}
~~~~
{% endtab %}
{% tab Java %}
~~~~ {prettyprint lang-java}
String streamName = "testStream";
try {
   stratioStreamingAPI.dropStream(streamName);
} catch(StratioStreamingException ssEx) {
    ssEx.printStackTrace();
}
~~~~
{% endtab %}
{% endtabgroup %}

Removing an existing query from a stream
========================================

{% tabgroup %}
{% tab Scala %}
~~~~ {prettyprint lang-java}
val streamName = "testStream"
val queryId = "alarms-657c1720-1869-4406-b42a-96b2b8f740b3"
try {
   stratioStreamingAPI.removeQuery(streamName, queryId)
} catch {
  case ssEx: StratioStreamingException => println(ssEx.printStackTrace())
}
~~~~
{% endtab %}
{% tab Java %}
~~~~ {prettyprint lang-java}
String streamName = "testStream";
String queryId = "alarms-f6bd870f-2cbb-4691-ba2c-ef4392e70a1b";
try {
   stratioStreamingAPI.removeQuery(streamName, queryId);
} catch(StratioStreamingException ssEx) {
    ssEx.printStackTrace();
}
~~~~
{% endtab %}
{% endtabgroup %}

Listening to streams
====================

{% tabgroup %}
{% tab Scala %}
~~~~ {prettyprint lang-java}
try {
  val streams = stratioStreamingAPI.listenStream("testStream")
  for(stream  {
         println("Column: "+column.getColumn)
         println("Value:"+column.getValue)
         println("Type: "+column.getType)}
      )
  }
} catch {
   case ssEx: StratioStreamingException => println(ssEx.printStackTrace())
}
~~~~
{% endtab %}
{% tab Java %}
~~~~ {prettyprint lang-java}
try {
  KafkaStream<String, StratioStreamingMessage> streams = stratioStreamingAPI.listenStream("testStream");
  for (MessageAndMetadata stream: streams) {
     StratioStreamingMessage theMessage = (StratioStreamingMessage)stream.message();
     for (ColumnNameTypeValue column: theMessage.getColumns()) {
        System.out.println("Column: "+column.getColumn());
        System.out.println("Value: "+column.getValue());
        System.out.println("Type: "+column.getType());
     }
  }
} catch(StratioStreamingException ssEx) {
    ssEx.printStackTrace();
}
~~~~
{% endtab %}
{% endtabgroup %}

Stop listening to streams
=========================

{% tabgroup %}
{% tab Scala %}
~~~~ {prettyprint lang-java}
try {
    stratioStreamingAPI.stopListenStream("testStream")
} catch {
   case ssEx: StratioStreamingException => println(ssEx.printStackTrace())
}
~~~~
{% endtab %}
{% tab Java %}
~~~~ {prettyprint lang-java}
try {
   stratioStreamingAPI.stopListenStream("testStream");
} catch(StratioStreamingException ssEx) {
   ssEx.printStackTrace();
}
~~~~
{% endtab %}
{% endtabgroup %}

Save the stream to Cassandra
============================

{% tabgroup %}
{% tab Scala %}
~~~~ {prettyprint lang-java}
try {
    stratioStreamingAPI.saveToCassandra("testStream")
} catch {
   case ssEx: StratioStreamingException => println(ssEx.printStackTrace())
}
~~~~
{% endtab %}
{% tab Java %}
~~~~ {prettyprint lang-java}
try {
   stratioStreamingAPI.saveToCassandra("testStream");
} catch(StratioStreamingException ssEx) {
   ssEx.printStackTrace();
}
~~~~
{% endtab %}
{% endtabgroup %}

Stop saving the stream to Cassandra
===================================

{% tabgroup %}
{% tab Scala %}
~~~~ {prettyprint lang-java}
try {
    stratioStreamingAPI.stopSaveToCassandra("testStream")
} catch {
   case ssEx: StratioStreamingException => println(ssEx.printStackTrace())
}
~~~~
{% endtab %}
{% tab Java %}
~~~~ {prettyprint lang-java}
try {
   stratioStreamingAPI.stopSaveToCassandra("testStream");
} catch(StratioStreamingException ssEx) {
   ssEx.printStackTrace();
}
~~~~
{% endtab %}
{% endtabgroup %}

Save the stream to MongoDB
==========================

{% tabgroup %}
{% tab Scala %}
~~~~ {prettyprint lang-java}
try {
    stratioStreamingAPI.saveToMongo("testStream")
} catch {
   case ssEx: StratioStreamingException => println(ssEx.printStackTrace())
}
~~~~
{% endtab %}
{% tab Java %}
~~~~ {prettyprint lang-java}
try {
   stratioStreamingAPI.saveToMongo("testStream");
} catch(StratioStreamingException ssEx) {
   ssEx.printStackTrace();
}
~~~~
{% endtab %}
{% endtabgroup %}

Stop saving the stream to MongoDB
=================================

{% tabgroup %}
{% tab Scala %}
~~~~ {prettyprint lang-java}
try {
    stratioStreamingAPI.stopSaveToMongo("testStream")
} catch {
   case ssEx: StratioStreamingException => println(ssEx.printStackTrace())
}
~~~~
{% endtab %}
{% tab Java %}
~~~~ {prettyprint lang-java}
try {
   stratioStreamingAPI.stopSaveToMongo("testStream");
} catch(StratioStreamingException ssEx) {
   ssEx.printStackTrace();
}
~~~~
{% endtab %}
{% endtabgroup %}

Index the stream to Elasticsearch
=================================

{% tabgroup %}
{% tab Scala %}
~~~~ {prettyprint lang-java}
try {
    stratioStreamingAPI.indexStream("testStream")
} catch {
   case ssEx: StratioStreamingException => println(ssEx.printStackTrace())
}
~~~~
{% endtab %}
{% tab Java %}
~~~~ {prettyprint lang-java}
try {
   stratioStreamingAPI.indexStream("testStream");
} catch(StratioStreamingException ssEx) {
   ssEx.printStackTrace();
}
~~~~
{% endtab %}
{% endtabgroup %}

Stop indexing the stream to Elasticsearch
=========================================

{% tabgroup %}
{% tab Scala %}
~~~~ {prettyprint lang-java}
try {
    stratioStreamingAPI.stopIndexStream("testStream")
} catch {
   case ssEx: StratioStreamingException => println(ssEx.printStackTrace())
}
~~~~
{% endtab %}
{% tab Java %}
~~~~ {prettyprint lang-java}
try {
   stratioStreamingAPI.stopIndexStream("testStream");
} catch(StratioStreamingException ssEx) {
   ssEx.printStackTrace();
}
~~~~
{% endtab %}
{% endtabgroup %}

Getting the list of all the streams and their queries
=====================================================

{% tabgroup %}
{% tab Scala %}
~~~~ {prettyprint lang-java}
import scala.collection.JavaConversions._

val listOfStreams = stratioStreamingAPI.listStreams().toList
println("Number of streams: "+listOfStreams.size)
listOfStreams.foreach(stream => {
   println("--> Stream name: "+stream.getStreamName)
   if ( stream.getQueries.size > 0 ) {
     stream.getQueries.foreach(query =>
       println("Query: "+query.getQuery))
   }
})
~~~~
{% endtab %}
{% tab Java %}
~~~~ {prettyprint lang-java}
List<StratioStream> streamsList = stratioStreamingAPI.listStreams();
System.out.println("Number of streams: " + streamsList.size());
for (StratioStream stream: streamsList) {
   System.out.println("--> Stream Name: "+stream.getStreamName());
   if ( stream.getQueries().size() > 0 ) {
      for (StreamQuery query: stream.getQueries())
        System.out.println("Query: "+query.getQuery());
      }
}
~~~~
{% endtab %}
{% endtabgroup %}
