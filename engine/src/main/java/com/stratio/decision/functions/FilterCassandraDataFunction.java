package com.stratio.decision.functions;

import com.stratio.decision.commons.constants.StreamAction;
import com.stratio.decision.commons.messages.StratioStreamingMessage;
import org.apache.spark.api.java.function.Function;
/**
 * Created by josepablofernandez on 8/06/16.
 */
public class FilterCassandraDataFunction implements Function<StratioStreamingMessage, Boolean> {

    @Override public Boolean call(StratioStreamingMessage v1) throws Exception {
        if (v1.getActiveActions().contains(StreamAction.SAVE_TO_CASSANDRA))
            return true;
        return false;
    }
}
