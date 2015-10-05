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

import com.stratio.decision.commons.constants.ReplyCode;
import com.stratio.decision.commons.constants.STREAM_OPERATIONS;
import com.stratio.decision.commons.constants.StreamAction;
import com.stratio.decision.commons.messages.StratioStreamingMessage;
import com.stratio.decision.functions.ActionBaseFunction;
import com.stratio.decision.functions.validator.*;
import com.stratio.decision.service.StreamOperationService;

import java.util.Set;

public class ListenStreamFunction extends ActionBaseFunction {
    private static final long serialVersionUID = 4566359991793310850L;

    public ListenStreamFunction(StreamOperationService streamOperationService, String zookeeperHost) {
        super(streamOperationService, zookeeperHost);
    }

    @Override
    protected String getStartOperationCommand() {
        return STREAM_OPERATIONS.ACTION.LISTEN;
    }

    @Override
    protected String getStopOperationCommand() {
        return STREAM_OPERATIONS.ACTION.STOP_LISTEN;
    }

    @Override
    protected boolean startAction(StratioStreamingMessage message) {
        getStreamOperationService().enableAction(message.getStreamName(), StreamAction.LISTEN);
        return true;
    }

    @Override
    protected boolean stopAction(StratioStreamingMessage message) {
        getStreamOperationService().disableAction(message.getStreamName(), StreamAction.LISTEN);
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
        validators.add(new ActionEnabledValidation(getStreamOperationService(), StreamAction.LISTEN,
                ReplyCode.KO_LISTENER_ALREADY_EXISTS.getCode()));
        validators.add(new StreamNotExistsValidation(getStreamOperationService()));
        validators.add(new KafkaStreamNameValidator());
    }
}
