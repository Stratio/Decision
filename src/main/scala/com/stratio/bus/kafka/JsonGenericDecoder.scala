package com.stratio.bus.kafka

import kafka.serializer.Decoder
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import java.util
import com.stratio.streaming.commons.messages.StratioStreamingMessage

class JsonGenericDecoder[T] extends Decoder[T] {
    def fromBytes(bytes: Array[Byte]): T = {
      val typeOfT = new TypeToken[StratioStreamingMessage](){}.getType()
      new Gson().fromJson(new String(bytes), typeOfT)
    }
}
