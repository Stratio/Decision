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

import java.net.UnknownHostException;
import java.util.Set;

import org.wso2.siddhi.core.SiddhiManager;

import com.stratio.streaming.commons.constants.REPLY_CODES;
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS;
import com.stratio.streaming.commons.constants.StreamAction;
import com.stratio.streaming.commons.messages.StratioStreamingMessage;
import com.stratio.streaming.exception.RequestValidationException;
import com.stratio.streaming.functions.ActionBaseFunction;
import com.stratio.streaming.functions.validator.ActionEnabledValidation;
import com.stratio.streaming.functions.validator.RequestValidation;
import com.stratio.streaming.functions.validator.StreamNotExistsValidation;
import com.stratio.streaming.streams.StreamOperations;

public class SaveToMongoStreamFunction extends ActionBaseFunction {

    private static final long serialVersionUID = -3553914914451458973L;

    private final String mongoHost;
    private final Integer mongoPort;
    private final String username;
    private final String password;

    public SaveToMongoStreamFunction(SiddhiManager siddhiManager, String zookeeperHost, String mongoHost,
            Integer mongoPort, String username, String password) {
        super(siddhiManager, zookeeperHost);
        this.mongoHost = mongoHost;
        this.mongoPort = mongoPort;
        this.username = username;
        this.password = password;
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
    protected boolean startAction(StratioStreamingMessage message) throws RequestValidationException {
        try {
            StreamOperations.save2mongoStream(message, mongoHost, mongoPort, username, password, getSiddhiManager());
            return true;
        } catch (UnknownHostException e) {
            throw new RequestValidationException(REPLY_CODES.KO_GENERAL_ERROR, e);
        }
    }

    @Override
    protected boolean stopAction(StratioStreamingMessage message) throws RequestValidationException {
        StreamOperations.stopSave2mongoStream(message, getSiddhiManager());
        return true;
    }

    @Override
    protected void addStopRequestsValidations(Set<RequestValidation> validators) {
        validators.add(new StreamNotExistsValidation(getSiddhiManager()));
    }

    @Override
    protected void addStartRequestsValidations(Set<RequestValidation> validators) {
        validators.add(new ActionEnabledValidation(getSiddhiManager(), StreamAction.SAVE_TO_MONGO,
                REPLY_CODES.KO_SAVE2MONGO_STREAM_ALREADY_ENABLED));
        validators.add(new StreamNotExistsValidation(getSiddhiManager()));
    }

}
