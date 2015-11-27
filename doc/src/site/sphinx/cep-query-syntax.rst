.. _cep-query-syntax:

Complex Event Processing Query Syntax
*************************************

Stratio Decision embedded the Siddhi Complex Event Processing Engine (Siddhi CEP). The Siddhi CEP uses the Siddhi Query Language (SiddhiQL), it is a declarative language designed for processing streams and identify complex event occurrences. The queries describe how to combine existing event streams information to create new event streams. When the queries are deployed in the Stratio Decision engine, those queries process incoming event streams filtering the data, and generates new output event streams if they don’t exist.

Syntax
======

The Siddhi language includes the following constructs:

- Event Definitions.
- Queries (Filters, Windows, Joins, Patterns, Sequences).
- Calls to external functions.

Siddhi Query Language definition:
::

    <query> ::= <define-stream> | <execution-query>
    <define-stream> ::= define stream <stream-name> <attribute-name> <type> {<attribute-name> <type>}
    <execution-query>::= <input> <output> [<projection>]
    <input> ::= from <streams>
    <output> ::= ((insert [<output-type>] into <stream-name>) | (return [<output-type>]))
    <streams> ::= <stream>[#<window>]
    |   <stream>#<window> [unidirectional] <join> [unidirectional] <stream>#<window> on <condition> within <time>
    |   [every] <stream> -> <stream> ... <stream> within <time>
    |  <stream>, <stream>, <stream> within <time>
    <stream> ::= <stream-name> <condition-list>
    <projection> ::= (<external-call> <attributelist>) | <attributelist> [group by <attribute-name> ][having <condition>]
    <external-call> ::= call <name> ( <param-list> )
    <condition-list> ::= {‘[’<condition>’]’}
    <attributelist>::=(<attribute-name> [as <reference-name>]) | ( <function>(<param-list>) as <reference-name>)
    <output-type> ::= expired-events | current-events | all-events
    <param-list> ::= {<expression>}
    <condition> ::= ( <condition> (and|or) <condition> ) | (not <condition>) | ( <expression> (==|!=|>=|<=|>|<|contains|instanceof) <expression> )
    <expression> ::= ( <expression> (+ | - | / | * | %) <expression> ) | <attribute-name> | <int> | <long> | <double> | <float> | <string> | <time>
    <time> ::= [<int>( years | year )]  [<int>( months | month )] [<int>( weeks | week )] [<int>( days | day )] [<int>( hours | hour )] [<int>( minutes | min | minute )] [<int>( seconds | second | sec )]
      [<int>( milliseconds | millisecond )]


Event Definitions
=================

The event stream definition defines the event stream schema. An event stream definition contains a unique name and a set of attributes assigned specific types, with uniquely identifiable names within the stream. Syntax:
::

    create --stream <stream name> --definition "<attribute name>.<attribute type>, <attribute name>.<attribute type>, ... "

Example:
::

    create --stream temperatureStream --definition "sensorId.long, name.string, temperature.double"

Queries
=======

Each Siddhi query can consume one or more event streams and create a new event stream from them. All queries contain an input section and an output section. A simple query with all three sections is as follows:
::

    add query --stream <output stream name> --definition
    "from <input stream name>
    select <attribute name>, <attribute name>, ...
    insert into <output stream name>"

Siddhi receives as input an stream of events. An event is similar to a database row, so it's like an array including
event values in the fields.
Using our previous example if we want to send only the temperature to the output stream the query can be defined as follows:

::

    add query --stream temperatureStream --definition
    "from temperatureStream select name, temperature insert into resultsStream"

In this case the resultsStream is a Inferred Stream. The resultsStream created can be used as an input query for another query without defining explicitly because the stream definition is inferred from the above query.


Query types supported by Siddhi
===============================

Pass-through
------------

Pass-through query creates an output stream according to the projection defined and inserts any events from the input stream to the output stream.
::

    add query --stream temperatureStream --definition
    "from temperatureStream select name, temperature insert into passthroughStream
    or
    from temperatureStream select name, temperature insert into passthroughStream *
    or
    from temperatureStream insert into passthroughStream name, temperature"

Filters
-------

Filter queries select data of given streams and insert the results in an output stream. Filter supports the following conditions:

- >, <, ==, >=, <=, !=
- contains, instanceof
- and, or, not

Examples:
::

    add query --stream temperatureStream --definition
    "from temperatureStream[name == 'hall' and temperature > 35] insert into highTemperatureAlarm
    or
    from temperatureStream[temperature < 10 or temperature > 35] insert into highTemperatureAlarm
    or
    from temperatureStream[temperature instanceof 'double'] insert into cleanTemperatureStream
    or
    from temperatureStream[name contains 'room'] insert into roomsTemperatureStream"

Other than that we can also use  instanceof condition for 'float', 'long' , 'integer', 'double' and 'boolean'.
Contains condition can only be applied to strings.


Windows
-------

A window is a limited subset of events from an event stream. Users can define windows and then use the events on the window calculations. A window has 2 types of output, current events and expired events. A window emits current events when new events arrives. Expired events are emitted whenever an existing event has expired from a window.

CEP queries can have 3 different output types ("current-events", "expired-events", "all-events"). Users can define these output types by adding  the proper keyword in between "insert" and "into" in the query syntax:

* **"current-events"** keyword. The output is only triggered when new events arrive at the window. Notifications will not be given when the expired events trigger the query from the window.
* **"expired-events"** keyword. The query emits output only when the expired events trigger it from the window and not from new events.
* **"all-events"** keyword. The query emits output when it is triggered by both newly-arrived and expired events from the window.
* No keyword is given. By default, the query assigns "current-events" to its output stream.


In output event streams, users can define aggregate functions to calculate aggregations within the defined window. CEP supports the following types of aggregate functions:

* sum
* avg
* max
* min
* count

Aggregate function must be named using ‘as’ keyword. Thus name can be used for referring that attribute, and will be used as the attribute name in the output stream.


There are different types of windows:

1. **Length Window**. Define a sliding window that keeps the last N events.
::

    add query --stream temperatureStream --definition "
    from temperatureStream[name=='hall']#window.length(100)
    select name, avg(temperature) as avgTemperature group by name having avgTemperature > 40
    insert into temperatureWLStream "

In the above example, from the events with name equals to 'hall' in temperatureStream with a window length of 100 elements, output the "current-events" and the output stream temperatureWLStream will have the average temperature when the average temperature is higher than 40.

2. **Length Batch Window**. Define a Length window that output events as a batch only at the Nth event arrival.
::

    add query --stream temperatureStream --definition "
    from temperatureStream[name == 'hall']#window.lengthBatch(100)
    select name, avg(temperature) as avgTemperature group by name having avgTemperature < 10
    insert into temperatureWLBatchStream for expired-events"

In the above example, from the events with name equals to 'hall' in temperatureStream, output the "expired-events" of the length batch window to the output stream temperatureWLBatchStream, that will have the average temperature when the average temperature lower than 10.

3. **Time Window**. Define a sliding window that keeps events arrived the last T time period.
::

    add query --stream temperatureStream --definition "
    from temperatureStream[name contains 'room']#window.time(15 min)
    select name, min(temperature) as minTemperature, max(temperature) as maxTemperature
    insert into temperatureTimeWindowStream "

In the above example, from the events where name contains "room" word in temperatureStream, output the "current-events" with max and min values in the selected Time Window (15 minutes).


4. **Time Batch Window**. Define a time window that processes events in batches. A loop collects the incoming events arrived within the last T period and outputs them as a batch.
::

    add query --stream temperatureStream --definition "
    from temperatureStream[name contains 'room']#window.timeBatch(30 min)
    select name, avg(temperature) as avgTemperature insert into temperatureTimeWindowStream "

In the above example, from the events where name contains "room" word in temperatureStream, output the temperature average of "current-events" in the output stream in the selected Time Batch Window (30 minutes).

5. **Unique Window**. Define a window that keeps only the latest events that are unique according to the given unique
attribute.
::

    add query --stream temperatureStream --definition "
    from temperatureStream#window.unique(name)
    select name, temperature insert into temperatureUniqueStream for expired-events"

In the above example, from the events of the temperatureStream, output the "expired-events" of the unique window to the output stream. Here, the output event is the immediate previous event having the same name of the current event.
    Unique Window is mostly used in Join Queries.

6. **First Unique Window**. Define a window that keeps the first event that are unique according to the given unique
attribute.
::

    add query --stream temperatureStream --definition "
    from temperatureStream#window.firstUnique(name)
    select name, temperature insert into temperatureFirstUniqueStream "

In the above example, from the events of the temperatureStream, output the "current-events" of the first unique window to the output stream. Here, the output event is the event arriving for each name. Also First Unique Window is mostly used in Join Queries.


**Supported Units For Time Windows**

The following units are supported when specifying the time for a time window. Note that each unit supports both the singular and plural format.

* Year. year | years
* Month. month | months
* Week. week | weeks
* Day. day | days
* Hour. hour | hours
* Minutes. minute | minutes | min
* Seconds. second | seconds | sec
* Milliseconds. millisecond | milliseconds


Joins
-----

Joins takes two streams as input associating both streams. Each stream must have associated a window, and generates the output events composed of ine event from each stream. Syntax:
::

    from <stream>#<window> [unidirectional]
        join <stream>#<window> [unidirectional]
    [on <condition>] [within <time>]
    insert [<output-type>] into <stream-name> ( {<attribute-name>}| ‘*’)


With "on <condition>" Siddhi joins only the events that matches the condition. With "within <time>" Siddhi joins only the events that are within that time of each other.
::

    add query --stream temperatureStream --definition "
    from temperatureStream[name == 'garage']#window.length(100) join
    otherStream#window.time(5 min) insert into joinStream "

In the above example, from the events of the temperatureStream with name equals to "garage" in a window with a length of 100 elements, join the events with the events of otherStream that has a time window of 5 minutes in the output stream with name joinStream.

Only inner join is supported in the current version of CEP. When we join two streams, the events arriving at either stream will trigger a joining process. The CEP also supports a special ‘unidirectional’ join. Here only one stream (the stream defined with the ‘unidirectional’ keyword ) will trigger the joining process.



Patterns
--------

Patterns processing is based in one or more input streams. Pattern matches events or conditions about events from input streams against a series of happen before or after relationships. The input event streams of the query should be referenced in order to uniquely identify events of those streams. Any event in the output stream is a collection of events received from input streams which satisfy the given pattern. For the pattern, the output attribute should be named using the "as" keyword.
::

    add query --stream temperatureStream --definition "
    from e1=temperatureStream[temperature >= 30] -> e2= otherStream[temperature > e1.temperature]
    select e1.name as name, e2.temperature as temperature insert into patternsStream"

In the above example, for the events of the temperatureStream with temperature >= to 30 followed by an event arrival having temperature higher than e1 temperature an output will be triggered via patternsStream stream.

Without every keyword the query will only run once. If you have the "every" enclosing a pattern, then the query runs for every occurrence of that pattern. Furthermore, if "within <time>" is used Siddhi triggers only the patterns where the first and the last events constituting to the pattern have arrived within the given time period.
::

    add query --stream temperatureStream --definition "
    from every(e1 = temperatureStream[name == 'hall'] -> e2 = humidityStream[humidity > 80])
    -> o1= everyMatchStream [ temperature > temperatureStream.temperature]
    within 10000 select e1.name as name, e2.humidity as humidity insert into everyStream"

In the above example, for every temperatureStream event with name equals followed an by a humidityStream event having humidity higher than 80, the everyMatchStream event will be matched when its temperature is > temperatureStream temperature. e1 and o1 should be within 1000 msec.

You can combine streams in patterns using logical OR and AND logical operators.
::

    add query --stream temperatureStream --definition "
    from every e1=temperatureStream[ name == 'hall'] and e2=temperatureStream2[name == 'hall']
    -> o1= mixTemperatureStream[ temperature >= 30 ] -> o2= mixTemperatureStream[ temperature >= 35 ]
    select e1.name as name, o1.temperature as temperatureA, o2.temperature as temperatureB
    insert into logicalSequenceStream"

In the above example, for the events of the streams temperatureStream and temperatureStream2 with name equals to "hall", the mixTemperatureStream will be matched for events having temperature >= 30 followed with events with temperature >= 35

Also you can count the number of event occurrences of the same event stream with the minimum and maximum limits. For example, <1:5> means 1 to 5 events, <2:> means 2 or more, and <3> means exactly 3 events. When referring to the results events matching the count pattern, square brackets should be used to access a specific occurrence of that event.
::

    add query --stream temperatureStream --definition "
    from every e1= temperatureStream[name == 'hall']<3> ->
    o1 = temperatureCountingStream[temperature > 30]<2:> ->
    o2 = temperatureCountingStream[temperature > 35]<1:5>
    select e1[0].name as name, b1.temperature as temperatureA, b2[4].temperature as temperatureB
    insert into temperatureResultCountStream "

In the above example, for 3 events in the temperatureStream with name "hall" the temperatureCountingStream will be matched with 2 or more events with temperature > 30 followed with 1 to 5 events with temperature > 35.


Sequences
---------

Sequences processing is based in one or more input streams. Sequences processing must exactly match the sequence of events without any other events in between. As input takes a sequence of conditions defined in a simple regular expression fashion. The events of the input streams should be assigned names in order to uniquely identify these events when constructing the query projection. It generates the output event stream such that any event in the output stream is a collection of events arrived from the input streams that exactly matches the order defined in the sequence. For a sequence, the output attribute must be named using the ‘as’ keyword, and it will be used as the output attribute name. When “within <time>” is used, just like with patterns, Siddhi will output only the events that are within that time of each other.

Following Regular Expressions are supported:

* Zero or more matches  (*)(reluctant).
* One or more matches (+) (reluctant).
* Zero or one match (?) (reluctant).

::

    add query --stream temperatureStream --definition "
    from e1= temperatureStream[ name == 'garage']+,
    o1= temperatureSeqStream[temperature > 30]?, o2= temperatureSeqStream[temperature >= 35]
    select e1[0].name as name, o1[0].temperature as tempA, o2[0].temperature as tempB
    insert into temperatureSeqOutputStream"

In the above example, for a sequence of one or more events with name equals to "garage", the query matches temperatureSeqStream events with maximum of 1 event with temperature > 30 and one event with temperature >= 35.


