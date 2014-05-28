package com.stratio.streaming.functions.validator;

import org.wso2.siddhi.core.SiddhiManager;

import com.stratio.streaming.commons.constants.StreamAction;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.exception.RequestValidationException;
import com.stratio.streaming.streams.StreamSharedStatus;

public class ActionEnabledValidation extends BaseSiddhiRequestValidation {

    private final static String ACTION_ALREADY_ENABLED = "Action %s already enabled";

    private final StreamAction streamAction;
    private final int specificErrorCode;

    public ActionEnabledValidation(SiddhiManager sm, StreamAction streamAction, int specificErrorCode) {
        super(sm);
        this.streamAction = streamAction;
        this.specificErrorCode = specificErrorCode;
    }

    @Override
    public void validate(StratioStreamingMessage request) throws RequestValidationException {
        if (StreamSharedStatus.getStreamStatus(request.getStreamName(), getSm()) != null) {
            if (StreamSharedStatus.getStreamStatus(request.getStreamName(), getSm()).isActionEnabled(streamAction)) {
                throw new RequestValidationException(specificErrorCode, String.format(ACTION_ALREADY_ENABLED,
                        streamAction));
            }
        }
    }

}
