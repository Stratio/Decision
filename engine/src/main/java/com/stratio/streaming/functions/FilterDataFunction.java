package com.stratio.streaming.functions;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

import com.stratio.streaming.commons.constants.StreamAction;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;

public class FilterDataFunction implements Function<Tuple2<StreamAction, Iterable<StratioStreamingMessage>>, Boolean> {

    private final StreamAction actionToFilter;

    public FilterDataFunction(StreamAction actionToFilter) {
        this.actionToFilter = actionToFilter;
    }

    @Override
    public Boolean call(Tuple2<StreamAction, Iterable<StratioStreamingMessage>> tuple) throws Exception {
        if (actionToFilter.equals(tuple._1)) {
            return true;
        } else {
            return false;
        }
    }
}
