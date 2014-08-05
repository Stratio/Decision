package com.stratio.streaming.streams;

import com.stratio.streaming.commons.constants.StreamAction;

public class QueryActionDTO {

    private StreamAction streamAction;
    private String actionQueryId;

    public QueryActionDTO() {
    }

    public QueryActionDTO(StreamAction streamAction, String actionQueryId) {
        this.streamAction = streamAction;
        this.actionQueryId = actionQueryId;
    }

    @Override
    public boolean equals(Object obj) {
        return streamAction.equals(obj);
    }

    public StreamAction getStreamAction() {
        return streamAction;
    }

    public String getActionQueryId() {
        return actionQueryId;
    }

}
