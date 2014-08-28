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

import java.util.Set;

import com.stratio.streaming.commons.constants.ReplyCode;
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS;
import com.stratio.streaming.commons.constants.StreamAction;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.functions.ActionBaseFunction;
import com.stratio.streaming.functions.validator.ActionEnabledValidation;
import com.stratio.streaming.functions.validator.MongoStreamColumnValidator;
import com.stratio.streaming.functions.validator.MongoStreamNameValidator;
import com.stratio.streaming.functions.validator.RequestValidation;
import com.stratio.streaming.functions.validator.StreamNotExistsValidation;
import com.stratio.streaming.service.StreamOperationService;

public class SaveToMongoStreamFunction extends ActionBaseFunction {

    private static final long serialVersionUID = -3553914914451458973L;

    public SaveToMongoStreamFunction(StreamOperationService streamOperationService, String zookeeperHost) {
        super(streamOperationService, zookeeperHost);
    }

    @Override
    protected String getStartOperationCommand() {
        return STREAM_OPERATIONS.ACTION.SAVETO_MONGO;
    }

    @Override
    protected String getStopOperationCommand() {
        return STREAM_OPERATIONS.ACTION.STOP_SAVETO_MONGO;
    }

    @Override
    protected boolean startAction(StratioStreamingMessage message) {
        getStreamOperationService().enableAction(message.getStreamName(), StreamAction.SAVE_TO_MONGO);
        return true;
    }

    @Override
    protected boolean stopAction(StratioStreamingMessage message) {
        getStreamOperationService().disableAction(message.getStreamName(), StreamAction.SAVE_TO_MONGO);
        return true;
    }

    @Override
    protected void addStopRequestsValidations(Set<RequestValidation> validators) {
        validators.add(new StreamNotExistsValidation(getStreamOperationService()));
    }

    @Override
    protected void addStartRequestsValidations(Set<RequestValidation> validators) {
        validators.add(new ActionEnabledValidation(getStreamOperationService(), StreamAction.SAVE_TO_MONGO,
                ReplyCode.KO_ACTION_ALREADY_ENABLED.getCode()));
        validators.add(new StreamNotExistsValidation(getStreamOperationService()));
        validators.add(new MongoStreamNameValidator());
        validators.add(new MongoStreamColumnValidator());
    }

}
