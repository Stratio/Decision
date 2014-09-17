# Stratio Streaming Examples

## Synopsis

In this module you can browse different code examples using the stratio streaming platform. You also can obtain a **VagrantFile** used to provisioning a virtual machine to execute the examples.

## Installation

#### Vagrant
To get an operating virtual machine with stratio streaming distribution up and running, we use [Vagrant](https://www.vagrantup.com/).

* Download and install Vagrant from here: https://www.vagrantup.com/downloads.html
* Copy the VagrantFile into any system folder.
* Execute **vagrant up**. The first time it executes, it must take a while.
* When the initialization script finish, you can access to the virtual machine typing **vagrant ssh**.
* To shutdown the virtual machine, use **vagrant halt**.
* For more options, type **vagrant**.
* This virtual machine executes:
      + Stratio Streaming
      + Stratio Streaming Shell
      + Apache Kafka
      + Apache Zookeeper
      + Stratio Cassandra
* By default the machine ip is: 10.10.10.10
* To validate that all is working, run **vagrant ssh**. Then, run **streaming-shell**. If you can see the streaming shell logo, all is working ok.

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
