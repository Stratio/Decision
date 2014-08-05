package com.stratio.streaming.functions;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

import com.stratio.streaming.commons.constants.StreamAction;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;

public class PairDataFunction implements
        PairFlatMapFunction<Iterator<StratioStreamingMessage>, StreamAction, StratioStreamingMessage> {

    private static final long serialVersionUID = -1456940258968414737L;

    @Override
    public Iterable<Tuple2<StreamAction, StratioStreamingMessage>> call(Iterator<StratioStreamingMessage> messages)
            throws Exception {
        Set<Tuple2<StreamAction, StratioStreamingMessage>> result = new HashSet<>();
        while (messages.hasNext()) {
            StratioStreamingMessage message = messages.next();
            for (StreamAction action : message.getActiveActions()) {
                result.add(new Tuple2<StreamAction, StratioStreamingMessage>(action, message));
            }
        }
        return result;
    }

}
