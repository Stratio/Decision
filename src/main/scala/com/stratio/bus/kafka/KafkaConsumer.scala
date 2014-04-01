package com.stratio.bus.kafka

import kafka.serializer._
import java.util.Properties
import kafka.utils.Logging
import scala.collection.JavaConversions._
import kafka.consumer.{KafkaStream, Consumer, ConsumerConfig, Whitelist}
import scala.concurrent.{ExecutionContext, Future}
import ExecutionContext.Implicits.global
import scala.Predef._

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

  val stream = connector.createMessageStreamsByFilter(filterSpec, 1, new DefaultDecoder(), new DefaultDecoder()).get(0)

  def read(write: (Array[Byte])=>Unit) = {
     Future {
      for(messageAndTopic <- stream) {
        try {
          write(messageAndTopic.message)
        } catch {
          case e: Throwable =>
            if (true) {
              error("Error processing message, skipping this message: ", e)
            } else {
              throw e
            }
        }
      }
     }

  }

  def close() {
    connector.shutdown()
  }
}
