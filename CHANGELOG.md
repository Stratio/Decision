# Changelog

Only listing significant user-visible, not internal code cleanups and minor bug fixes.

## 0.10.0 (Upcoming)

* Various documentation improvements
* Removed transitive dependencies because licenses

## 0.9.0 (October 2015)

* Product name changed to Stratio Decision
* CEP syntax documentation added
* Minor bugs fixed
* Multiple Integration and unit tests added

## 0.8.1 (September 2015)

* Update deb and rpm generation.

## 0.8.0 (August 2015)

* Failover implemented using Zookeeper, no need for a running Cassandra instance when starting the engine.
* Streaming Engine now takes care of its initialization status, allowing the shell to show more precise error messages.
* Upgraded ElasticSearch version (1.7.1)

## 0.7.1 (July 2015)

* Fixed problem with documentation generation.

## 0.7.0 (July 2015)

* New feature save to Solr.
* New feature high availability with Zookeeper.
* (issue #36, issue #17) Update Spark version to 1.3.1.
* (issue #30) Fixed problem with config files.
* (issue #35) Fixed failover problems.
* (issue #27) Base topics are now created when the engine starts.
* (issue #28) Fixed connection problems with Stratio Ingestion.

## 0.6.2 (January 2015)

* (issue #19) Error retrieving long values using the API: Default Gson parser converts any number to Double.
* (issue #20) Default Gson parser converts any number to Double.
* (issue #21) ColumnNameTypeValueDeserializer have issue when the jsonvalue is null.

## 0.6.1 (December 2014)

* API: Fixed a bug in new close() method.

## 0.6.0 (December 2014)

* Added streams with engine statistics. Now, you can create dashboards or manage the streaming status like other datasource
* Configuration changes to accept an array of hosts in all datastores. Also we have been added the elasticsearch cluster name into configuration.
* Before, when you execute shell and the engine isn´t running, the shell crash resoundingly. Now, you have a nice error if you run a command and engine is not connected
* Fixed some error descriptions into shell.
* Added close method to API.
* Fix some bugs.

## 0.5.0 (Octubre 2014)

* Updated Cassandra driver to version 2.1.0
* Updated ElasticSearch driver to version 1.3.2
* Updated Spark to version 1.0.2
* Added a new module with examples
