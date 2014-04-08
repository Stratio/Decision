package com.stratio.bus.kafka

import kafka.serializer.Decoder
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import java.util
import com.stratio.streaming.commons.messages.StratioStreamingMessage

class JsonGenericDecoder extends Decoder[StratioStreamingMessage] {
    def fromBytes(bytes: Array[Byte]): StratioStreamingMessage = {
      //val typeOfT = new TypeToken[util.Collection[StratioStreamingMessage]](){}.getType()
      new Gson().fromJson(new String(bytes), classOf[StratioStreamingMessage])
    }
}
