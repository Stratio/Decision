package com.stratio.streaming.functions;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.factory.GsonFactory;
import com.stratio.streaming.serializer.impl.KafkaToJavaSerializer;

public class SerializerFunction implements Function<Tuple2<String, String>, StratioStreamingMessage> {

    private static final long serialVersionUID = 1150382218319606622L;

    private KafkaToJavaSerializer kafkaToJavaSerializer;

    @Override
    public StratioStreamingMessage call(Tuple2<String, String> object) throws Exception {
        return getKafkaToJavaSerializer().serialize(object._2());
    }

    private KafkaToJavaSerializer getKafkaToJavaSerializer() {
        if (kafkaToJavaSerializer == null) {
            kafkaToJavaSerializer = new KafkaToJavaSerializer(GsonFactory.getInstance());
        }
        return kafkaToJavaSerializer;
    }
}
