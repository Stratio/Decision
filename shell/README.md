# _Streaming shell_

_Basic shell to interact with streaming engine_

## Project Setup

_This project was created with maven._ 

## Deploying

### _How to setup the deployment environment_

- _On the project folder, execute 'mvn clean assembly:assembly'._
- _A 'tar.gz' binary is generated into 'target' folder._
- _Execute '/bin/shell' in unix systems or '/bin/shell.bat' in windows._
- _To change properies, edit '/conf/shell.properties' file_

### _Available commands_
The Shell has autocomplete function pressing TAB.

* add query - Create new query. 
```
Example:
add query --stream test --definition "from test #window.length(10) select * insert into test2"
```
* columns - Get all metadata from stream.
```
Example:
columns --stream test
```
* create - Create new stream.
```
Example:
create --stream test --definition "col1.INTEGER,col2.STRING"
```
* drop - Drop existing stream.
```
Example:
drop --stream test
```
* insert - insert events into existing stream
```
Example:
insert --stream test --values "col1.234,col2.testString"
```
* alter - Alter existing stream
* list - list all streams into engine
* listen start - attach stream to kafka topic
* listen stop - de-attach stream to kafka topic
* remove query - remove an existing query
* index start - index stream events
* index stop - stop index stream events
* save cassandra start - start save to cassandra action
* save cassandra stop - stop save to cassandra action
* save mongo start - start save to mongo action
* save mongo stop - stop save to mongo action
* send drools start - start send data to drools action
* send drools stop - stop send data to drools action

## License

Copyright (C) 2014 Stratio (http://stratio.com)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
        http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

http://www.apache.org/licenses/LICENSE-2.0.html
