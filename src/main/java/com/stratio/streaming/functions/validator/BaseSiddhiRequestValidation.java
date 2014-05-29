package com.stratio.streaming.functions.validator;

import org.wso2.siddhi.core.SiddhiManager;

public abstract class BaseSiddhiRequestValidation implements RequestValidation {

    private final SiddhiManager sm;

    public BaseSiddhiRequestValidation(SiddhiManager sm) {
        super();
        this.sm = sm;
    }

    public SiddhiManager getSm() {
        return sm;
    }

}
