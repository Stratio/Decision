package com.stratio.decision.functions.engine;

import org.wso2.siddhi.core.event.Event;

/**
 * Created by josepablofernandez on 2/12/15.
 */
public interface EngineAction {

    public void execute(String streamName, Event[] events);
}
