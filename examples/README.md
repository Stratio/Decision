# Stratio Streaming Examples

## Synopsis

In this module you can browse different code examples using the Stratio Streaming platform. You also can obtain a **Vagrantfile** used to provisioning a virtual machine to execute the examples.

## Installation

### Vagrant
To get an operating virtual machine with stratio streaming distribution up and running, we use [Vagrant](https://www.vagrantup.com/).

* Download and install Vagrant from here: https://www.vagrantup.com/downloads.html
* If you not have been VirtualBox already installed we need install them: https://www.virtualbox.org/wiki/Downloads
* Download [this Vagrantfile](https://raw.githubusercontent.com/Stratio/stratio-streaming/develop/examples/Vagrantfile).
* If you are in a windows machine, we will install [Cygwin](https://cygwin.com/install.html)
* Copy the Vagrantfile into any system folder with exactly the same name, **Vagrantfile**.
* Inside this directory, execute **vagrant up**. The first time it executes, it must take a while.
* When the initialization script finish, you can access to the virtual machine typing **vagrant ssh**.
* To shutdown the virtual machine, use **vagrant halt**.
* For more options, type **vagrant**.
* This virtual machine executes:
      + Stratio Streaming
      + Stratio Streaming Shell
      + Apache Kafka
      + Apache Zookeeper
      + Stratio Cassandra
      + Elasticsearch
      + Kibana
      + Mongodb
* By default the machine ip is: 10.10.10.10
* To validate that everything is working, run **vagrant ssh**. Then, run **streaming-shell**. If you can see the streaming shell logo, everything is working ok.

#### FAQs
* **I am in the same directory that I copy the Vagrant file but I have this error:**
```
      A Vagrant environment or target machine is required to run this
      command. Run `vagrant init` to create a new Vagrant environment. Or,
      get an ID of a target machine from `vagrant global-status` to run
      this command on. A final option is to change to a directory with a
      Vagrantfile and to try again.
```
      Make sure your file name is **Vagrantfile** instead of _**Vagrantfile.txt**_ or _**VagrantFile**_.

### Examples
There are some examples to generate example data.
* To build a binary with the examples execute **_mvn clean package_** in root project.
* Go to **_examples_** module, into the target folder.
* Uncompress file **_stratio-examples-0.5.0-SNAPSHOT-app.tar.gz_**.

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
