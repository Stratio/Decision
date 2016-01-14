package com.stratio.decision.functions.dal;

import java.util.Set;

import com.stratio.decision.commons.constants.EngineActionType;
import com.stratio.decision.commons.constants.ReplyCode;
import com.stratio.decision.commons.constants.STREAM_OPERATIONS;
import com.stratio.decision.commons.constants.StreamAction;
import com.stratio.decision.commons.messages.StratioStreamingMessage;
import com.stratio.decision.functions.ActionBaseFunction;
import com.stratio.decision.functions.validator.ActionEnabledValidation;
import com.stratio.decision.functions.validator.DroolsStreamNameValidator;
import com.stratio.decision.functions.validator.RequestValidation;
import com.stratio.decision.functions.validator.StreamNameNotEmptyValidation;
import com.stratio.decision.functions.validator.StreamNotExistsValidation;
import com.stratio.decision.service.StreamOperationService;

/**
 * Created by josepablofernandez on 21/12/15.
 */
public class SendToDroolsStreamFunction extends ActionBaseFunction  {

    public SendToDroolsStreamFunction(StreamOperationService streamOperationService, String zookeeperHost) {
        super(streamOperationService, zookeeperHost);
    }

    @Override
    protected String getStartOperationCommand() {
        return STREAM_OPERATIONS.ACTION.START_SENDTODROOLS;
    }

    @Override
    protected String getStopOperationCommand() {
        return STREAM_OPERATIONS.ACTION.STOP_SENDTODROOLS;
    }

    @Override
    protected boolean startAction(StratioStreamingMessage message) {

        getStreamOperationService().enableEngineAction(message.getStreamName(), EngineActionType.FIRE_RULES, message.getAdditionalParameters());
        return true;
    }

    @Override
    protected boolean stopAction(StratioStreamingMessage message) {
        getStreamOperationService().disableEngineAction(message.getStreamName(), EngineActionType.FIRE_RULES);
        return true;
    }

    @Override
    protected void addStopRequestsValidations(Set<RequestValidation> validators) {
        validators.add(new StreamNameNotEmptyValidation());
        validators.add(new StreamNotExistsValidation(getStreamOperationService()));
    }

    @Override
    protected void addStartRequestsValidations(Set<RequestValidation> validators) {
        validators.add(new StreamNameNotEmptyValidation());
        validators.add(new ActionEnabledValidation(getStreamOperationService(), StreamAction.SEND_TO_DROOLS,
                ReplyCode.KO_ACTION_ALREADY_ENABLED.getCode()));
        validators.add(new StreamNotExistsValidation(getStreamOperationService()));
        validators.add(new DroolsStreamNameValidator());
    }

}
