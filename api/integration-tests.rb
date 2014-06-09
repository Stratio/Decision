#
# Copyright (C) 2014 Stratio (http://stratio.com)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

packageProject = "mvn package -DskipTests=true"
system(packageProject)

scalaTestRunnerCommand = "scala -cp /home/albertorodriguez/.m2/repository/org/scalatest/scalatest_2.10/2.1.0/scalatest_2.10-2.1.0.jar:target/streaming-api-jar-with-dependencies.jar org.scalatest.tools.Runner -o -R target/test-classes -s com.stratio.streaming.integration.StratioStreamingIntegrationTests -DzookeeperHost=172.19.0.228 -DzookeeperPort=2181 -DkafkaHost=172.19.0.228 -DkafkaPort=9092 -DelasticSearchHost=172.19.0.228 -DelasticSearchPort=9200 -DcassandraHost=172.19.0.228 -DmongoHost=172.19.0.228"
if ARGV.length == 1
  scalaTestRunnerCommand << " -n \"" << ARGV[0] << "\""
end

system(scalaTestRunnerCommand)

