.. _stratio-streaming-sandbox:

Stratio Streaming sandbox and demo
**********************************

Vagrant Setup
=============

To get an operating virtual machine with stratio streaming distribution
up and running, we use `Vagrant <https://www.vagrantup.com/>`_.

-  Download and install
   `Vagrant <https://www.vagrantup.com/downloads.html>`_.
-  Download and install
   `VirtualBox <https://www.virtualbox.org/wiki/Downloads>`_.
-  If you are in a windows machine, we will install
   `Cygwin <https://cygwin.com/install.html>`_.

Running the sandbox
===================

Create any system folder and using the command line, type
**vagrant init stratio/streaming**.

To facilitate the reading of the document , we will refer to this
directory as /install-folder.

Please, be patient the first time it runs.

 .. image:: /images/vagrant-shell.png
    :align: center

What you will find in the sandbox
=================================

-  OS: CentOS 6.5
-  3GB RAM - 2 CPU
-  Two ethernet interfaces.

Name\|Version\|Service name\|Other Stratio
Streaming\|{{site.projects`2].version}}\|stratio-streaming\|service
streaming start Stratio Streaming
Shell\|{{site.projects`2].version}}\|-\|/opt/sds/streaming-shell/bin
Apache Kafka\|0.8.1.1\|kafka\|service kafka start Apache
Zookeeper\|3.4.6\|zookeeper\|service zookeeper start Stratio
Cassandra\|2.1.05\|cassandra\|service cassandra start
Elasticsearch\|1.3.2\|elasticsearch\|service elasticsearch start
Kibana\|3.1.0\|-\|\ http://10.10.10.10/kibana
Mongodb\|2.6.5\|mongod\|service mongod start Apache Web
Server\|2\|httpd\|service httpd start

Access to the sandbox and other useful commands
===============================================

Useful commands
---------------

-  Start the sandbox: **\` vagrant up \`**
-  Shut down the sandbox: **\` vagrant halt \`**
-  In the sandbox, to exit to the host: **\` exit \`**

Accessing the sandbox
---------------------

-  Located in /install-folder
-  **\` vagrant ssh \`**

Starting the Stratio Streaming Shell and other useful commands
==============================================================

From the sandbox (vagrant ssh):

-  Starting the Stratio Streaming Shell:
   **/opt/sds/streaming-shell/bin/shell**
-  List all available commands: **help**
-  Exit the Stratio Stratio Streaming Shell: **exit**

F.A.Q about the sandbox
=======================

**I am in the same directory that I copy the Vagrant file but I
have this error:**::

    A Vagrant environment or target machine is required to run this
    command. Run vagrant init to create a new Vagrant environment. Or,
    get an ID of a target machine from vagrant global-status to run
    this command on. A final option is to change to a directory with a
    Vagrantfile and to try again.

Make sure your file name is Vagrantfile instead of Vagrantfile.txt or
VagrantFile.

**When I execute vagrant ssh I have this error:**::

     ssh executable not found in any directories in the %PATH% variable. Is an
     SSH client installed? Try installing Cygwin, MinGW or Git, all of which
     contain an SSH client. Or use your favorite SSH client with the following
     authentication information shown below:

We need to install `Cygwin <https://cygwin.com/install.html>`_ or `Git for
Windows <http://git-scm.com/download/win>`_.

Stratio Streaming Demos
=======================

Demo #1: Sensor Monitoring
--------------------------

This demo will show up some of the features of Stratio Streaming, an
interactive CEP engine built with Apache Spark and Apache Siddhi, such
as:

-  Use time-based and event-length sliding windows (with aggregation
   functions)
-  Launch some queries in order to control thresholds and insert events
   in other streams
-  The use of derived streams (streams whose definition is inferred by
   the engine, because it is implicit in the queries)
-  Start actions on streams

This CEP use case is oriented to track measures in a sensor monitoring
environment, but Complex Event Processing could be succesfully applied
to other scenarios and use cases, such as fraud detection, real-time
applications and systems monitoring, stock-quote analysis or
ciber-security, among others.

In this demo, you will use the following components and features:

-  The Stratio Streaming Engine, taking care of all the real-time
   processing and all the CEP operations.
-  The Stratio Streaming Shell, in order to interact with the engine in
   real-time.
-  The Stratio Streaming API, in order to send simulated sensor measures
   to the engine.
-  The INDEX action over several streams, in order to send all the
   events in a stream to a data storage, in this case Elastic Search.
-  Kibana web application as a real-time monitor of the entire system.

To put all these pieces to work, you need to:

-  Write some commands in the Stratio Streaming Shell to create all the
   streams, queries and actions.
-  Simulate some random sensor measures related to basic signals of a
   system (cpu, memory, processes…)
-  And lastly, visualize all the indexed data in real-time.

Shell steps
-----------

-  vagrant ssh
-  /opt/sds/streaming-shell/bin/shell

- Creation of a base stream, where we are going to insert all the sensor
measures. A stream definition is similar to a table, with field
definition and types::

     create --stream sensor_grid --definition "name.string,data.double"

- **List** command allow us to check out the current state of the CEP
engine. How many streams and queries are already created?, Which actions
are enabled on a stream?, What is the definition of a stream?::

    list

- By launching this query we are aggregating the sensor measures in windows based on event length (250 events), so that
we can get an average measure by each sensor type. This is a continuous query, it will be registered from now in the engine, unlike the classic
request/response model of the relational databases. In addition, the result of the query will be inserted in another stream, whose definition
is inferred from the query’s projection. That means that you don’t have to explicitly create the output stream. The engine will infer the
definition of the stream and create it automatically.::

     add query --stream sensor_grid --definition "from sensor_grid#window.length(250) select name, avg(data) as data group by name insert into sensor_grid_avg  for current-events"

- We request the engine to start one of the available actions on the
base stream that we have previously created. In particular the one that
send all the events in this stream to Elastic Search. Actions can be
enabled and disabled in any moment, and there are actions ready to use
such us saving the events into Cassandra, MongoDB or ElasticSearch. In
addition, there is an special action called LISTEN that send events to
an specific topic on Kafka whose name is the same as the stream in which
the action has been enabled.::

     index start --stream sensor_grid_avg

- Now, by doing a “list”, we can check out that there are two streams, one query and the
stream called “sensor\_grid” has an action enabled, INDEX.::

     list

- Now, working on the aggregated measures, we will launch two queries that use operators to filter and set thresholds on
events. Furthermore, we will use time-based windows to fire alarms if these thresholds are reached only in an specific period of time. The
output of these queries is sent to the same new stream, again infered by the engine.

- Query #1::

     add query --stream sensor_grid_avg --definition "from sensor_grid_avg`name=='cpu' and data > 80]#window.timeBatch(10 seconds)  select name, avg(data) as data, 'Alarm_intensive_CPU_load' as text insert into sensor_grid_alarms  for current-events"

- Query #2::

     add query --stream sensor_grid_avg --definition "from sensor_grid_avg`name=='memory' and data > 75]#window.timeBatch(5 seconds)  select name, avg(data) as data, 'Alarm_intensive_MEMORY_load' as text insert into sensor_grid_alarms  for current-events"

- Query #3::

     add query --stream sensor_grid_avg --definition "from sensor_grid_avg`(name=='memory' and data > 80) or (name=='cpu' and data > 90)]#window.timeBatch(15 seconds) select name , avg(data) as data, 'Alarm_inminent_shutdown' as text insert into sensor_grid_alarms for current-events"

- Let’s start indexing the alarms, too::

     index start --stream sensor_grid_alarms

- If you want, you can start inserting one event by using the shell::

     insert --stream sensor_grid --values "name.cpu,data.33"

- We are done with the shell.::

    exit

Sensor grid simulation steps
----------------------------

- Now, let’s send some bulk data to the engine. All the measures are
fake but we are producing random variations on them, in order to
simulate the behaviour of a real system::

     sudo sh  /opt/sds/streaming-examples/bin/hardware-emulator 2 streaming.stratio.com:9092

- You can launch this tool as many times as you want.

Dashboard steps
---------------

-  Open a browser on your machine and go here: `http://[SANDBOX\_IP]/kibana/index.html#/dashboard/file/sensor-grid-monitoring.json <http://[SANDBOX_IP]/kibana/index.html#/dashboard/file/sensor-grid-monitoring.json>`_
-  Thanks to this real-time dashboard, you can watch all the things
   happening inside the engine. All the aggregated events, alarms in
   some fancy widgets.

 .. image:: /images/kibana-sensor-grid-dashboard.png
    :align: center

Extra: Streaming metrics
------------------------

Stratio Streaming is the result of combining the power of Spark
Streaming as a continuous computing framework and Siddhi CEP engine as
complex event processing engine. This dashboard is showing some
statistics related to the status of the Stratio Streaming engine,
allowing you to inspect commands, events and throughput, in a real-time
panel. This way, we took advantage of the engine itself to take care of
all the internal events produced by the engine. In order to get this
dashboard working, please execute the following commands:

- To start we need change some properties into streaming engine
configuration.::

    sudo vi /etc/sds/streaming/config.conf
- Set **statsEnabled** property to true.
- Now, is necessary to restart streaming service.::

     sudo service streaming restart

- Using the shell, execute this commands::

      /opt/sds/streaming-shell/bin/shell

- You can execute into shell the list command and you should obtain this result::

     stratio-streaming> list
     Stream name                  User defined  Queries  Elements  Active actions
     ---------------------------  ------------  -------  --------  --------------
     streaming-gauge-metrics      false         0        3         []
     streaming-counter-metrics    false         0        3         []
     streaming-histogram-metrics  false         0        13        []
     streaming-meter-metrics      false         0        8         []
     streaming-timer-metrics      false         0        19        []

- Execute this commands to index all metric streams::

     index start --stream streaming-gauge-metrics
     index start --stream streaming-meter-metrics
     index start --stream streaming-counter-metrics
     index start --stream streaming-histogram-metrics
     index start --stream streaming-timer-metrics

- Now, you can access to metrics kibana dashboard:
     http://[SANDBOX_IP]/kibana/index.html#/dashboard/file/streaming-status.json
