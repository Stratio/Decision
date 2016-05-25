/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
//        validators.add(new StreamNotExistsValidation(getStreamOperationService()));
        validators.add(new StreamNotExistsValidation());
    }

    @Override
    protected void addStartRequestsValidations(Set<RequestValidation> validators) {
        validators.add(new StreamNameNotEmptyValidation());
//        validators.add(new ActionEnabledValidation(getStreamOperationService(), StreamAction.SEND_TO_DROOLS,
//                ReplyCode.KO_ACTION_ALREADY_ENABLED.getCode()));
//        validators.add(new StreamNotExistsValidation(getStreamOperationService()));
//        validators.add(new DroolsStreamNameValidator());

        validators.add(new ActionEnabledValidation(StreamAction.SEND_TO_DROOLS,
                ReplyCode.KO_ACTION_ALREADY_ENABLED.getCode()));
        validators.add(new StreamNotExistsValidation());
        validators.add(new DroolsStreamNameValidator());
    }

}
