.. _using-stratio-streaming-api:

Using Stratio Streaming API
***************************

Creating and initializing the API
=================================

Scala::

   scala val stratioStreamingAPI = StratioStreamingAPIFactory.create().initialize

Java::

   IStratioStreamingAPI stratioStreamingAPI = StratioStreamingAPIFactory.create().initialize();

Creating and initializing the API with server configuration
===========================================================

Scala::

   val stratioStreamingAPI = StratioStreamingAPIFactory.create()
        .initializeWithServerConfig("stratio.node.com", 9092, "stratio.node.com", 2181)

Java::

   IStratioStreamingAPI stratioStreamingAPI = StratioStreamingAPIFactory.create()
        .initializeWithServerConfig("stratio.node.com", 9092, "stratio.node.com", 2181);

Creating a new stream
=====================

Scala::

    val firstStreamColumn = new ColumnNameType("column1", ColumnType.INTEGER)
    val secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING)
    val streamName = "testStream"
    val columnList = Seq(firstStreamColumn, secondStreamColumn)
    try {
        stratioStreamingAPI.createStream(streamName, columnList)
    } catch {
        case ssEx: StratioStreamingException => println(ssEx.printStackTrace())
    }

Java::

    ColumnNameType firstStreamColumn= new ColumnNameType("column1", ColumnType.INTEGER);
    ColumnNameType secondStreamColumn = new ColumnNameType("column2", ColumnType.STRING);
    String streamName = "testStream";
    List columnList = Arrays.asList(firstStreamColumn, secondStreamColumn);
    try {
          stratioStreamingAPI.createStream(streamName, columnList);
    } catch (StratioStreamingException e) {
          e.printStackTrace();
    }

Adding columns to an existing stream
====================================

Scala::

    val newStreamColumn = new ColumnNameType("column3", ColumnType.DOUBLE)
    val streamName = "testStream"
     val columnList = Seq(newStreamColumn)
     try {
            stratioStreamingAPI.alterStream(streamName, columnList)
     } catch {
            case ssEx: StratioStreamingException => println(ssEx.printStackTrace())
     }

Java::

    ColumnNameType thirdStreamColumn= new ColumnNameType("column3", ColumnType.DOUBLE);
    String streamName = "testStream";
    List columnList = Arrays.asList(thirdStreamColumn);
    try {
         stratioStreamingAPI.alterStream(streamName, columnList);
    } catch (StratioStreamingException e) {
         e.printStackTrace();
    }

Inserting data into a stream
============================

Scala::

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

Java::

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

Adding queries to streams
=========================

Scala::

    val query = "from testStream select column1, column2, column3 insert into alarms for current-events"
    val streamName = "testStream"
    try {
        val queryId = stratioStreamingAPI.addQuery(streamName, query)
    } catch {
       case ssEx: StratioStreamingException => println(ssEx.printStackTrace())
    }

Java::

    String streamName = "testStream";
    String query = "from testStream select column1, column2, column3 insert into alarms for current-events";
    try {
        String queryId = stratioStreamingAPI.addQuery(streamName, query);
    } catch(StratioStreamingException ssEx) {
        ssEx.printStackTrace();
    }

Removing an existing stream
===========================

Scala::

    val streamName = "testStream"
    try {
         stratioStreamingAPI.dropStream(streamName)
    } catch {
        case ssEx: StratioStreamingException => println(ssEx.printStackTrace())
    }

Java::

    String streamName = "testStream";
    try {
        stratioStreamingAPI.dropStream(streamName);
    } catch(StratioStreamingException ssEx) {
         ssEx.printStackTrace();
    }

Removing an existing query from a stream
========================================

Scala::

    val streamName = "testStream"
    val queryId = "alarms-657c1720-1869-4406-b42a-96b2b8f740b3"
    try {
        stratioStreamingAPI.removeQuery(streamName, queryId)
    } catch {
       case ssEx: StratioStreamingException => println(ssEx.printStackTrace())
    }

Java::

    String streamName = "testStream";
    String queryId = "alarms-f6bd870f-2cbb-4691-ba2c-ef4392e70a1b";
    try {
        stratioStreamingAPI.removeQuery(streamName, queryId);
    } catch(StratioStreamingException ssEx) {
         ssEx.printStackTrace();
    }

Listening to streams
====================

Scala::

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

Java::

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

Stop listening to streams
=========================

Scala::

    try {
        stratioStreamingAPI.stopListenStream("testStream")
    } catch {
       case ssEx: StratioStreamingException => println(ssEx.printStackTrace())
    }

Java::

    try {
       stratioStreamingAPI.stopListenStream("testStream");
    } catch(StratioStreamingException ssEx) {
       ssEx.printStackTrace();
    }

Save the stream to Cassandra
============================

Scala::

    try {
        stratioStreamingAPI.saveToCassandra("testStream")
    } catch {
       case ssEx: StratioStreamingException => println(ssEx.printStackTrace())
    }

Java::

    try {
       stratioStreamingAPI.saveToCassandra("testStream");
    } catch(StratioStreamingException ssEx) {
       ssEx.printStackTrace();
    }


Getting the list of all the streams and their queries
=====================================================

Scala::

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

Java::

    List<StratioStream> streamsList = stratioStreamingAPI.listStreams();
    System.out.println("Number of streams: " + streamsList.size());
    for (StratioStream stream: streamsList) {
       System.out.println("--> Stream Name: "+stream.getStreamName());
       if ( stream.getQueries().size() > 0 ) {
          for (StreamQuery query: stream.getQueries())
            System.out.println("Query: "+query.getQuery());
          }
    }
