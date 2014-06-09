/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.streaming.kafka

import kafka.serializer.{StringDecoder, DefaultDecoder}
import java.util.Properties
import kafka.utils.Logging
import scala.collection.JavaConversions._
import kafka.consumer.{KafkaStream, Consumer, ConsumerConfig, Whitelist}
import scala.Predef._
import com.stratio.streaming.commons.messages.StratioStreamingMessage
import scala.reflect.ClassTag

class KafkaConsumer(topic: String,
                     zookeeperConnect: String,
                     groupId: String = "1111",
                     readFromStartOfStream: Boolean = true
                     ) extends Logging {

  val props = new Properties()
  props.put("group.id", groupId)
  props.put("zookeeper.connect", zookeeperConnect)
  props.put("auto.offset.reset", if(readFromStartOfStream) "smallest" else "largest")

  val config = new ConsumerConfig(props)
  val connector = Consumer.create(config)

  val filterSpec = new Whitelist(topic)


  val stream = connector.createMessageStreamsByFilter(filterSpec, 1, new StringDecoder(), new JsonGenericDecoder()).get(0)

  def close() {
    connector.shutdown()
  }
}
