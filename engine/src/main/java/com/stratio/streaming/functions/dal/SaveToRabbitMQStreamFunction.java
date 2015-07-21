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
package com.stratio.streaming.functions.dal;

import com.stratio.streaming.commons.constants.ReplyCode;
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS;
import com.stratio.streaming.commons.constants.StreamAction;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.exception.RequestValidationException;
import com.stratio.streaming.functions.ActionBaseFunction;
import com.stratio.streaming.functions.validator.*;
import com.stratio.streaming.service.StreamOperationService;

import java.util.Set;

public class SaveToRabbitMQStreamFunction extends ActionBaseFunction {

    private static final long serialVersionUID = -5881469481191697432L;

    public SaveToRabbitMQStreamFunction(StreamOperationService streamOperationService, String zookeeperHost) {
        super(streamOperationService, zookeeperHost);
    }

    @Override
    protected String getStartOperationCommand() {
        return STREAM_OPERATIONS.ACTION.SAVETO_RABBITMQ;
    }

    @Override
    protected String getStopOperationCommand() {
        return STREAM_OPERATIONS.ACTION.STOP_SAVETO_RABBITMQ;
    }

    @Override
    protected boolean startAction(StratioStreamingMessage message) throws RequestValidationException {
        getStreamOperationService().enableAction(message.getStreamName(), StreamAction.SAVE_TO_RABBIT_MQ);
        return true;
    }

    @Override
    protected boolean stopAction(StratioStreamingMessage message) throws RequestValidationException {
        getStreamOperationService().disableAction(message.getStreamName(), StreamAction.SAVE_TO_RABBIT_MQ);
        return false;
    }

    @Override
    protected void addStopRequestsValidations(Set<RequestValidation> validators) {
        validators.add(new StreamNameNotEmptyValidation());
        validators.add(new StreamNotExistsValidation(getStreamOperationService()));
    }

    @Override
    protected void addStartRequestsValidations(Set<RequestValidation> validators) {
        validators.add(new StreamNameNotEmptyValidation());
        validators.add(new ActionEnabledValidation(getStreamOperationService(), StreamAction.SAVE_TO_RABBIT_MQ,
                ReplyCode.KO_ACTION_ALREADY_ENABLED.getCode()));
        validators.add(new StreamNotExistsValidation(getStreamOperationService()));
    }

}
