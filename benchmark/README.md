PERFORMANCE SUITE
===================

This performance suite should be able to run a defined set of tests to validate the proper performance of Stratio Streaming application.
The performance suite will do the following steps:

* Start a Zookeeper Docker Instance
* Start a Kafka Docker Instance
* Start a Decision Docker instance
* Load the required configuration queries
* Run the code to load data thru Kafka
* Get the Decision stats using JMX
* Get the result count reading from Kafka
* Shutdown the environment



# Boot auxiliary containers (first time)
docker run -d -p 172.17.42.1:53:53/udp --restart=always --name skydns crosbymichael/skydns -nameserver 8.8.8.8:53 -domain dev
docker run -d -v /var/run/docker.sock:/docker.sock --restart=always --name skydock crosbymichael/skydock -ttl 30 -environment local -s /docker.sock -domain dev -name skydns

docker pull qa.stratio.com:5000/stratio/streaming:latest


# Start existing images
docker run -d -p 172.17.42.1:53:53/udp --restart=always  -i crosbymichael/skydns -nameserver 8.8.8.8:53 -domain dev
docker run -d -v /var/run/docker.sock:/docker.sock --restart=always  -i crosbymichael/skydock -ttl 30 -environment local -s /docker.sock -domain dev -name skydns

