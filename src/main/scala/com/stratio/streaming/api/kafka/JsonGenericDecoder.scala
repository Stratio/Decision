package com.stratio.streaming.kafka

import kafka.serializer.Decoder
import com.google.gson.Gson
import com.stratio.streaming.commons.messages.StratioStreamingMessage

class JsonGenericDecoder extends Decoder[StratioStreamingMessage] {
    def fromBytes(bytes: Array[Byte]): StratioStreamingMessage = {
      new Gson().fromJson(new String(bytes), classOf[StratioStreamingMessage])
    }
}
