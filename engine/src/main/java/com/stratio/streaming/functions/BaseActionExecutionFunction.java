package com.stratio.streaming.functions;

import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

import com.stratio.streaming.commons.constants.StreamAction;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;

public abstract class BaseActionExecutionFunction implements
        Function<JavaPairRDD<StreamAction, Iterable<StratioStreamingMessage>>, Void> {

    private static final long serialVersionUID = -7719763983201600088L;

    static final String TIMESTAMP_FIELD = "timestamp";

    @Override
    public Void call(JavaPairRDD<StreamAction, Iterable<StratioStreamingMessage>> rdd) throws Exception {

        List<Tuple2<StreamAction, Iterable<StratioStreamingMessage>>> rddContent = rdd.collect();
        if (rddContent.size() != 0) {
            process(rddContent.get(0)._2);
        }
        return null;
    }

    public abstract void process(Iterable<StratioStreamingMessage> messages) throws Exception;
}
