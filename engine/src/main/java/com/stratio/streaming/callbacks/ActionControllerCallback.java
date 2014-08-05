package com.stratio.streaming.callbacks;

import java.util.Set;

import org.wso2.siddhi.core.query.output.callback.QueryCallback;

import com.stratio.streaming.commons.constants.StreamAction;

public abstract class ActionControllerCallback extends QueryCallback {

    protected final Set<StreamAction> activeActions;

    public ActionControllerCallback(Set<StreamAction> activeActions) {
        this.activeActions = activeActions;
    }

}
