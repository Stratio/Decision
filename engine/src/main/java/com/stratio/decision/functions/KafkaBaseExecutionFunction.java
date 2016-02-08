package com.stratio.decision.functions;

import java.util.Properties;

import com.stratio.decision.commons.messages.StratioStreamingMessage;
import com.stratio.decision.factory.GsonFactory;
import com.stratio.decision.serializer.Serializer;
import com.stratio.decision.serializer.impl.KafkaToJavaSerializer;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

/**
 * Created by aitor on 4/11/15.
 */
public abstract class KafkaBaseExecutionFunction extends BaseActionExecutionFunction {

    protected Producer<String, String> producer;
    protected KafkaToJavaSerializer kafkaToJavaSerializer;

    protected String kafkaQuorum;


    protected Serializer<String, StratioStreamingMessage> getSerializer() {
        if (kafkaToJavaSerializer == null) {
            kafkaToJavaSerializer = new KafkaToJavaSerializer(GsonFactory.getInstance());
        }
        return kafkaToJavaSerializer;
    }

    protected Producer<String, String> getProducer() {
        if (producer == null) {
            Properties properties = new Properties();
            properties.put("serializer.class", "kafka.serializer.StringEncoder");
            properties.put("metadata.broker.list", kafkaQuorum);
            properties.put("producer.type", "async");
            producer = new Producer<String, String>(new ProducerConfig(properties));
        }
        return producer;
    }

}
